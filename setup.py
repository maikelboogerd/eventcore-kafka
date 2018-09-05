import setuptools


setuptools.setup(
    name='eventcore-kafka',
    version='0.3.1',
    description='Produce and consume events with Kafka.',
    author='Maikel van den Boogerd',
    author_email='maikelboogerd@gmail.com',
    url='https://github.com/maikelboogerd/eventcore-kafka',
    keywords=['event', 'kafka', 'producer', 'consumer'],
    packages=['eventcore_kafka'],
    install_requires=['eventcore', 'confluent-kafka'],
    license='MIT',
    zip_safe=False
)
