"""Climate component for Itho add-on: Itho Spider room thermostats (RF).

One climate entity is created per Spider that shows up in the add-on's
`<base topic>/remotesinfo` MQTT message. Nothing is hard coded: a remote is
treated as a Spider when its capabilities contain both `temp` and `setpoint`.
"""

import json

from homeassistant.components import mqtt
from homeassistant.components.climate import (
    ClimateEntity,
    ClimateEntityFeature,
    HVACMode,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import ATTR_TEMPERATURE, UnitOfTemperature
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
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
)


def _is_spider(capabilities) -> bool:
    """A remote is a Spider thermostat if it reports a room temp and a setpoint."""
    return (
        isinstance(capabilities, dict)
        and "temp" in capabilities
        and "setpoint" in capabilities
    )


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
    _attr_hvac_modes = [HVACMode.HEAT]
    _attr_hvac_mode = HVACMode.HEAT
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
        self._state_topic = get_mqtt_remote_topic(config_entry.data)

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

    def _apply(self, payload: dict) -> None:
        """Update state from a remotesinfo payload (does not write state)."""
        capabilities = payload.get(self._remote_name)
        if not _is_spider(capabilities):
            self._attr_available = False
            return

        self._attr_available = True
        self._attr_current_temperature = capabilities.get("temp")
        self._attr_target_temperature = capabilities.get("setpoint")
        self._attr_extra_state_attributes = {
            "battery": capabilities.get("battery"),
            "presence": bool(capabilities.get("pir")),
        }
        # The add-on lists remotes in slot order and skips empty slots; the
        # position in the message is the `rfremoteindex` the add-on expects.
        self._remote_index = list(payload).index(self._remote_name)

    async def async_added_to_hass(self) -> None:
        """Subscribe to MQTT events."""
        self.async_on_remove(
            await mqtt.async_subscribe(
                self.hass,
                self._state_topic,
                self._message_received,
                MQTT_DEFAULT_QOS_SUBSCRIBE,
            )
        )

    @callback
    def _message_received(self, msg) -> None:
        """Handle remotesinfo update."""
        try:
            payload = json.loads(msg.payload)
        except ValueError:
            return
        if isinstance(payload, dict):
            self._apply(payload)
            self.async_write_ha_state()

    async def async_set_temperature(self, **kwargs) -> None:
        """Send a new setpoint to the Spider via the add-on."""
        temperature = kwargs.get(ATTR_TEMPERATURE)
        if temperature is None:
            return

        await mqtt.async_publish(
            self.hass,
            self._command_topic,
            json.dumps(
                {"rfsetpoint": float(temperature), "rfremoteindex": self._remote_index}
            ),
            MQTT_DEFAULT_QOS_PUBLISH,
        )
        # Show the new value right away; the add-on confirms via remotesinfo
        self._attr_target_temperature = temperature
        self.async_write_ha_state()

    async def async_set_hvac_mode(self, hvac_mode: HVACMode) -> None:
        """Only HEAT is supported; nothing to do."""
