import asyncio
import logging
import sys

from asyncua import Server, ua
from asyncua.server.internal_session import InternalSession
from asyncua.server.internal_subscription import InternalSubscription
from asyncua.ua.uaerrors import UaStatusCodeError

logging.basicConfig(level=logging.ERROR)
fault = ""
release = asyncio.Event()
original_subscription = InternalSession.create_subscription
original_items = InternalSession.create_monitored_items
original_activate = InternalSession.activate_session
original_republish = InternalSubscription.republish
rejected_session = None


def republish(subscription, sequence):
    global fault
    if fault == "republish_notification":
        fault = ""
        message = ua.NotificationMessage()
        message.SequenceNumber = sequence
        data = ua.DataChangeNotification()
        item = ua.MonitoredItemNotification()
        item.ClientHandle = 1
        item.Value = ua.DataValue(ua.Variant(123))
        data.MonitoredItems = [item]
        message.NotificationData = [data]
        return message
    return original_republish(subscription, sequence)


def activate_session(session, *args, **kwargs):
    global fault, rejected_session
    if fault == "reject_activate":
        rejected_session = session
        fault = ""
    if rejected_session is session:
        raise UaStatusCodeError(ua.StatusCodes.BadIdentityTokenInvalid)
    return original_activate(session, *args, **kwargs)


async def create_subscription(session, *args, **kwargs):
    global fault
    if fault == "reject_subscription":
        fault = ""
        raise UaStatusCodeError(ua.StatusCodes.BadTooManySubscriptions)
    if fault in ("drop_subscription_response", "drop_subscription_disconnect"):
        disconnect = fault == "drop_subscription_disconnect"
        fault = ""
        result = await original_subscription(session, *args, **kwargs)
        if disconnect:
            for protocol in list(current_server.bserver.clients):
                protocol.transport.close()
        await asyncio.sleep(1.5)
        return result
    return await original_subscription(session, *args, **kwargs)


async def create_items(session, *args, **kwargs):
    global fault
    parameters = args[0]
    reject_persistently = fault == "reject_items_always" and any(item.ItemToMonitor.NodeId.Identifier == 1002 for item in parameters.ItemsToCreate)
    if fault == "reject_items" or reject_persistently:
        if fault == "reject_items":
            fault = ""
        raise UaStatusCodeError(ua.StatusCodes.BadTooManyOperations)
    if fault == "hold_items":
        fault = ""
        print("RESTORING", flush=True)
        await release.wait()
    result = await original_items(session, *args, **kwargs)
    if fault == "reject_item":
        fault = ""
        result[0].StatusCode = ua.StatusCode(ua.StatusCodes.BadOutOfMemory)
    return result


InternalSession.create_subscription = create_subscription
InternalSession.create_monitored_items = create_items
InternalSession.activate_session = activate_session
InternalSubscription.republish = republish


async def start_server(endpoint, missing=False):
    global current_server
    server = Server()
    await server.init()
    server.set_endpoint(endpoint)
    await server.register_namespace("urn:taosdata:recovery-test")
    variables = []
    for node_id in (1001, 1002, 1003):
        if missing and node_id == 1002:
            continue
        variables.append(await server.nodes.objects.add_variable(ua.NodeId(node_id, 2), f"Counter{node_id}", 0))
    await server.start()
    current_server = server
    return server, variables


async def main():
    global fault
    endpoint = sys.argv[1]
    server, variables = await start_server(endpoint)
    print("READY", flush=True)
    command_task = asyncio.create_task(asyncio.to_thread(sys.stdin.readline))
    counter = 0
    try:
        while True:
            if command_task.done():
                command = command_task.result().strip()
                if command == "quit" or not command:
                    break
                if command.startswith("restart"):
                    await server.stop()
                    await asyncio.sleep(0.4)
                    fault = command.removeprefix("restart_") if command != "restart" else ""
                    release.clear()
                    server, variables = await start_server(endpoint, command == "restart_missing")
                    print("RESTARTED", flush=True)
                elif command in ("cut", "cut_reject_activate", "cut_republish"):
                    if command == "cut_reject_activate":
                        fault = "reject_activate"
                    if command == "cut_republish":
                        fault = "republish_notification"
                    for protocol in list(server.bserver.clients):
                        protocol.transport.close()
                    print("CUT", flush=True)
                elif command == "release":
                    release.set()
                    print("RELEASED", flush=True)
                elif command == "stats":
                    print(f"SESSIONS={len(server.iserver._external_sessions)}", flush=True)
                elif command == "subscriptions":
                    print(f"SUBSCRIPTIONS={len(server.iserver.subscription_service.subscriptions)}", flush=True)
                command_task = asyncio.create_task(asyncio.to_thread(sys.stdin.readline))
            counter += 1
            for variable in variables:
                await variable.write_value(counter)
            await asyncio.sleep(0.1)
    finally:
        release.set()
        await server.stop()


asyncio.run(main())
