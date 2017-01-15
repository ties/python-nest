"""
A **dirty** example of how to push events from the Nest streaming API to an
MQTT broker. Works for me...

Dependencies:
  * hbmqtt
"""
import json
import logging

import os

import asyncio
from hbmqtt.client import MQTTClient

import nest

DEFAULT_DELAY = 2
MAX_ERRORS = 8

MQTT_HOST = os.environ.get('MQTT_HOST', '172.16.0.2')
MQTT_PORT = os.environ.get('MQTT_PORT', 1883)

NEST_CLIENT_ID = os.environ.get('NEST_CLIENT_ID', None)
NEST_CLIENT_SECRET = os.environ.get('NEST_CLIENT_SECRET', None)


def ensure_nest_client():
    assert(NEST_CLIENT_ID and NEST_CLIENT_SECRET)
    # set-up nest, reading token cache from working dir
    nestc = nest.Nest(
        access_token_cache_file='./nest.cache',
        client_id=NEST_CLIENT_ID,
        client_secret=NEST_CLIENT_SECRET,
    )

    try:
        if nestc.access_token is None:
            nestc.request_token(os.environ.get('NEST_REQUEST_TOKEN', ''))
    except:
        print(nestc.authorize_url)
        raise ValueError('Nest not authorized')

    return nestc

#
# Use HBMQTT for mqtt w/ asyncio
#
async def publish_data(C, data):
    """
    Extract and publish the data from the API message
    @param C: mqtt client
    @param data: content of the API message
    """
    for id, thermostat in data['devices']['thermostats'].items():
        try:
            time_to_target = int(thermostat['time_to_target'].replace('~', ''))
        except:
            time_to_target = None

        body = json.dumps({
                'ambient_temperature': thermostat['ambient_temperature_c'],
                'humidity': thermostat['humidity'],
                'heating': int(thermostat['hvac_state'] == 'heating'),
                'eco': int(thermostat['hvac_state'] == 'eco'),
                'off': int(thermostat['hvac_state'] == 'off'),
                'target_temperature': thermostat['target_temperature_c'],
                'time_to_target': time_to_target
        })
        await C.publish('sensors/thermostat', body.encode('utf8'))

    for id, structure in data['structures'].items():
        body = json.dumps({ 'away': int(structure['away'] == 'away') })
        await C.publish('sensors/nest_away', body.encode('utf8'))

async def publish_nest_events(nestc, host, port=1883):
    C = MQTTClient()
    await C.connect('mqtt://{host}:{port}/'.format(host=host, port=port))

    log.info("Starting reader loop")
    errors = 0
    while errors < MAX_ERRORS:
        try:
            async with nestc.stream() as nest_events:
                async for (event, data) in nest_events:
                    if data and data.get('data', False):
                        data = data['data']

                        await publish_data(C, data)

                    delay = DEFAULT_DELAY
                    errors = 0
        except asyncio.TimeoutError:
            log.error('Timeout while reading, sleeping {}'.format(delay))
            await asyncio.sleep(delay)

            errors += 1
            delay *= 2

if __name__ == '__main__':
    # set-up logging
    logging.basicConfig()

    log = logging.getLogger('temperature_influxdb')
    log.setLevel(logging.INFO)

    nestc = ensure_nest_client()

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(publish_nest_events(nestc, MQTT_HOST))
    finally:
        loop.close()
