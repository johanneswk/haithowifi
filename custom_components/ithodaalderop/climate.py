"""Climate component for Itho add-on: Itho Spider room thermostats (RF).

One climate entity is created per Spider that shows up in the add-on's
`<base topic>/remotesinfo` MQTT message. Nothing is hard coded: a remote is
treated as a Spider when its capabilities contain both `temp` and `setpoint`.

A battery Spider only takes a new setpoint at its next poll (minutes), so the
entity keeps the requested value as "pending" and shows that it is still
waiting, until the Spider confirms it or the add-on gives up.

Heating / cooling / idle is derived from the system state of the AutoTemp on
the same add-on (`<base topic>/ithostatus`) and from the Spider's own values:
heating when the room is below its setpoint, cooling when it is above its
cooling setpoint.
"""

import json
import time

from homeassistant.components import mqtt
from homeassistant.components.climate import (
    ClimateEntity,
    ClimateEntityFeature,
    HVACAction,
    HVACMode,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import ATTR_TEMPERATURE, UnitOfTemperature
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.event import async_call_later
from homeassistant.util import slugify

from .const import (
    _LOGGER,
    CONF_ADDON_TYPE,
    DOMAIN,
    MANUFACTURER,
    MQTT_DEFAULT_QOS_PUBLISH,
    MQTT_DEFAULT_QOS_SUBSCRIBE,
)
from .utils import (
    get_device_model,
    get_device_name,
    get_entity_prefix,
    get_mqtt_command_topic,
    get_mqtt_remote_topic,
    get_mqtt_state_topic,
)

# Wait this long after the last +/- press before sending, so quick presses
# are sent as one command.
SEND_DELAY = 3  # seconds
# The add-on keeps a setpoint pending for 15 minutes; wait slightly longer.
PENDING_TIMEOUT = 16 * 60  # seconds after sending
# After sending, give the add-on this long to publish its own state before a
# different `setpoint_cmd` is taken as "dropped".
DROP_GRACE = 30  # seconds
# Mode shown while the room is neither heating nor cooling. OFF makes the card
# grey. If your card disables the +/- buttons in that state, use
# HVACMode.HEAT_COOL instead.
IDLE_MODE = HVACMode.OFF


def _is_spider(capabilities) -> bool:
    """A remote is a Spider thermostat if it reports a room temp and a setpoint."""
    return (
        isinstance(capabilities, dict)
        and "temp" in capabilities
        and "setpoint" in capabilities
    )


def _same(a, b) -> bool:
    """Compare two temperatures."""
    return a is not None and b is not None and abs(float(a) - float(b)) < 0.05


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Create a climate entity for every Spider the add-on reports."""

    # Only the autotemp add-on has Spiders (RF module needed)
    if config_entry.data[CONF_ADDON_TYPE] != "autotemp":
        return

    if not await mqtt.async_wait_for_mqtt_client(hass):
        _LOGGER.error("MQTT integration is not available")
        return

    known: set[str] = set()

    @callback
    def _discover(msg) -> None:
        try:
            payload = json.loads(msg.payload)
        except ValueError:
            return
        if not isinstance(payload, dict):
            return

        new = []
        for name, capabilities in payload.items():
            if name not in known and _is_spider(capabilities):
                known.add(name)
                new.append(IthoSpiderThermostat(name, payload, config_entry))
        if new:
            async_add_entities(new)

    config_entry.async_on_unload(
        await mqtt.async_subscribe(
            hass,
            get_mqtt_remote_topic(config_entry.data),
            _discover,
            MQTT_DEFAULT_QOS_SUBSCRIBE,
        )
    )


class IthoSpiderThermostat(ClimateEntity):
    """Room thermostat backed by an Itho Spider paired to the add-on."""

    _attr_has_entity_name = True
    _attr_should_poll = False
    _attr_temperature_unit = UnitOfTemperature.CELSIUS
    # The mode follows what the room is doing right now, because the mode
    # drives the colour and wording of the card (heat = orange, cool = blue,
    # off = grey). It cannot be set by hand; the AutoTemp decides.
    _attr_hvac_modes = list(
        dict.fromkeys([HVACMode.OFF, HVACMode.HEAT, HVACMode.COOL, IDLE_MODE])
    )
    _attr_hvac_mode = IDLE_MODE
    _attr_supported_features = ClimateEntityFeature.TARGET_TEMPERATURE
    _attr_min_temp = 5.0
    _attr_max_temp = 30.0
    _attr_target_temperature_step = 0.5

    def __init__(
        self, remote_name: str, initial_payload: dict, config_entry: ConfigEntry
    ) -> None:
        """Initialize the thermostat."""
        self._remote_name = remote_name
        self._remote_index = 0
        self._command_topic = get_mqtt_command_topic(config_entry.data)
        self._remote_topic = get_mqtt_remote_topic(config_entry.data)
        self._status_topic = get_mqtt_state_topic(config_entry.data)

        self._caps: dict | None = None  # latest Spider data
        self._status: dict | None = None  # latest AutoTemp status

        # Setpoint the user asked for that the Spider has not confirmed yet
        self._pending: float | None = None
        self._sent_at: float | None = None  # None until the command is sent
        self._cancel_send = None

        self._attr_name = remote_name
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, get_entity_prefix(config_entry.data))},
            manufacturer=MANUFACTURER,
            model=get_device_model(config_entry.data),
            name=get_device_name(config_entry.data),
        )
        self._attr_unique_id = (
            f"{get_entity_prefix(config_entry.data)}_spider_{slugify(remote_name)}"
        ).lower()
        self.entity_id = f"climate.{self._attr_unique_id}"

        self._apply(initial_payload)

    # ---- presentation -------------------------------------------------

    @property
    def icon(self) -> str | None:
        """Hourglass while waiting; otherwise the default heating/cooling icon."""
        return "mdi:timer-sand" if self._pending is not None else None

    @property
    def extra_state_attributes(self) -> dict:
        """Extra attributes, including whether the setpoint is still pending."""
        caps = self._caps or {}
        return {
            "setpoint_status": (
                "waiting for Spider" if self._pending is not None else "applied"
            ),
            "pending_setpoint": self._pending,
            "setpoint_cool": caps.get("setpoint_cool"),
            "system_state": self._system_state(),
            "battery": caps.get("battery"),
            "presence": bool(caps["pir"]) if "pir" in caps else None,
        }

    # ---- state --------------------------------------------------------

    def _system_state(self) -> str | None:
        """System-wide state of the AutoTemp."""
        status = self._status
        if not status:
            return None
        if status.get("state hand") == 1:
            return "hand"
        if status.get("State heating") == 1:
            return "heating"
        if status.get("State cool") == 1:
            return "cooling"
        if status.get("State off") == 1:
            return "off"
        return "idle"

    def _action(self) -> HVACAction | None:
        """Heating, cooling or idle for this room."""
        state = self._system_state()
        caps = self._caps
        if state is None or caps is None:
            return None

        temp = caps.get("temp")
        if state == "heating":
            setpoint = caps.get("setpoint")
            if temp is not None and setpoint is not None and temp < setpoint:
                return HVACAction.HEATING
            return HVACAction.IDLE
        if state == "cooling":
            cool = caps.get("setpoint_cool")
            if temp is not None and cool is not None and temp > cool:
                return HVACAction.COOLING
            return HVACAction.IDLE
        if state == "off":
            return HVACAction.OFF
        return HVACAction.IDLE

    def _refresh(self) -> None:
        """Recalculate everything from the latest data (does not write state)."""
        caps = self._caps
        if caps is None:
            self._attr_available = False
            return

        reported = caps.get("setpoint")
        commanded = caps.get("setpoint_cmd")

        # Is the pending setpoint done?
        if self._pending is not None and self._sent_at is not None:
            waited = time.monotonic() - self._sent_at
            if _same(reported, self._pending):
                self._pending = None  # the Spider took it
            elif waited > PENDING_TIMEOUT:
                self._pending = None  # the add-on stopped trying
            elif (
                waited > DROP_GRACE
                and commanded is not None
                and not _same(commanded, self._pending)
            ):
                _LOGGER.debug(
                    "%s: pending setpoint dropped by the add-on", self._remote_name
                )
                self._pending = None  # e.g. changed on the Spider itself

        self._attr_available = True
        self._attr_current_temperature = caps.get("temp")
        # Keep showing the requested value while it is pending
        self._attr_target_temperature = (
            self._pending if self._pending is not None else reported
        )
        self._attr_hvac_action = self._action()
        self._attr_hvac_mode = {
            HVACAction.HEATING: HVACMode.HEAT,
            HVACAction.COOLING: HVACMode.COOL,
        }.get(self._attr_hvac_action, IDLE_MODE)

    def _apply(self, payload: dict) -> None:
        """Update from a remotesinfo payload (does not write state)."""
        capabilities = payload.get(self._remote_name)
        self._caps = capabilities if _is_spider(capabilities) else None
        if self._caps is not None:
            # The add-on lists remotes in slot order and skips empty slots; the
            # position in the message is the `rfremoteindex` it expects.
            self._remote_index = list(payload).index(self._remote_name)
        self._refresh()

    # ---- MQTT ---------------------------------------------------------

    async def async_added_to_hass(self) -> None:
        """Subscribe to MQTT events."""
        self.async_on_remove(
            await mqtt.async_subscribe(
                self.hass,
                self._remote_topic,
                self._remote_message,
                MQTT_DEFAULT_QOS_SUBSCRIBE,
            )
        )
        self.async_on_remove(
            await mqtt.async_subscribe(
                self.hass,
                self._status_topic,
                self._status_message,
                MQTT_DEFAULT_QOS_SUBSCRIBE,
            )
        )

    async def async_will_remove_from_hass(self) -> None:
        """Cancel a scheduled send."""
        if self._cancel_send:
            self._cancel_send()
            self._cancel_send = None

    @callback
    def _remote_message(self, msg) -> None:
        """Handle a remotesinfo update."""
        try:
            payload = json.loads(msg.payload)
        except ValueError:
            return
        if isinstance(payload, dict):
            self._apply(payload)
            self.async_write_ha_state()

    @callback
    def _status_message(self, msg) -> None:
        """Handle an AutoTemp status update."""
        try:
            payload = json.loads(msg.payload)
        except ValueError:
            return
        if isinstance(payload, dict):
            self._status = payload
            self._refresh()
            self.async_write_ha_state()

    # ---- commands -----------------------------------------------------

    async def async_set_temperature(self, **kwargs) -> None:
        """Remember the new setpoint and send it once the user stopped changing it."""
        temperature = kwargs.get(ATTR_TEMPERATURE)
        if temperature is None:
            return

        self._pending = float(temperature)
        self._sent_at = None  # not sent yet
        self._refresh()
        self.async_write_ha_state()

        # Restart the delay on every change so +/- presses are sent as one command
        if self._cancel_send:
            self._cancel_send()
        self._cancel_send = async_call_later(self.hass, SEND_DELAY, self._send_pending)

    async def _send_pending(self, _now) -> None:
        """Send the pending setpoint to the Spider via the add-on."""
        self._cancel_send = None
        if self._pending is None:
            return

        await mqtt.async_publish(
            self.hass,
            self._command_topic,
            json.dumps(
                {"rfsetpoint": self._pending, "rfremoteindex": self._remote_index}
            ),
            MQTT_DEFAULT_QOS_PUBLISH,
        )
        self._sent_at = time.monotonic()
        self._refresh()
        self.async_write_ha_state()

    async def async_set_hvac_mode(self, hvac_mode: HVACMode) -> None:
        """The mode follows the AutoTemp and cannot be set."""
        raise ServiceValidationError(
            "The mode follows what the AutoTemp is doing and cannot be changed"
        )
