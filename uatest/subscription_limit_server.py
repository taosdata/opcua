#!/usr/bin/env python3

from opcua import Server, ua
from opcua.common.utils import ServiceError


if __name__ == "__main__":
    server = Server()
    server.set_endpoint("opc.tcp://0.0.0.0:4840/")

    subscription_service = server.iserver.subscription_service
    create_subscription = subscription_service.create_subscription

    def create_limited_subscription(params, callback):
        if subscription_service.subscriptions:
            raise ServiceError(ua.StatusCodes.BadTooManySubscriptions)
        return create_subscription(params, callback)

    subscription_service.create_subscription = create_limited_subscription
    server.start()
