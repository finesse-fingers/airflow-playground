
import socket
import json

from confluent_kafka import Producer


def main():
    conf = {'bootstrap.servers': "localhost:19092",
            'client.id': socket.gethostname()}
    print('conf:', conf)

    producer = Producer(conf)

    # topic = 'test.v1'
    topic = 'customer.onboarding.v1.event'
    key = None
    message = json.dumps({
        "metadata": {
            "id": "12d2772c-d05b-4c7f-961d-d94a13f783be",
            "source": "urn:nz:co:kiwibank:df:migration:dev",
            "type": "onboardingCreated",
            "subject": "TBD: $4b387f34-edf3-4acb-aef9-4ca92ec81c75",
            "time": "2021-06-23T22:02:06.147644+00:00",
            "correlationId": "c6e3f806-a218-4953-a77d-f8b4771d806a",
            "actor": "TBD"
        },
        "data": {
            "beforeState": None,
            "afterState": {
                "onboardingId": "4b387f34-edf3-4acb-aef9-4ca92ec81c75",
                "journeyId": "30c427a4-66d2-41ae-926c-40da5200292b",
                "firstName": "Hello",
                "lastName": "World",
                "email": "shaun.fuchs@kiwibank.local",
                "mobileNumber": "0221891548",
                "dateOfBirth": "2021-12-31",
                "onboardingState": "onboardingCompletedSuccessfully"
            }
        }
    })
    headers = {
        # "cap-callback-name": None,
        # "cap-msg-id": "1407821289620512770",
        # "cap-corr-id": "1407821289620512770",
        # "cap-corr-seq": "0",
        "cap-msg-name": "customer.onboarding.v1.event",
        # "cap-msg-type": "OnboardingEvent",
        # "cap-senttime": "24/06/2021 10:02:06 +12:00",
        # "kb-schema-id": "2",
        # "cap-msg-group": "migration.test.v1"
    }

    print(f"Producing record: ({key}, {message}, {headers})")
    producer.produce(topic, value=message, key=key,
                     headers=headers, on_delivery=acked)

    # p.poll() serves delivery reports (on_delivery)
    # from previous produce() calls.
    producer.poll(0)
    producer.flush()


def acked(err, msg):
    """
    Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        print("Produced record to topic {} partition [{}] @ offset {}"
              .format(msg.topic(), msg.partition(), msg.offset()))


if __name__ == "__main__":
    main()
