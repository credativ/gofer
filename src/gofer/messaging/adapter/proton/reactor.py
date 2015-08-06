from time import time
from collections import deque
from uuid import uuid4

from proton import Url, Timeout, Endpoint
from proton import ProtonException, ConnectionException, LinkException
from proton.reactor import Container, Delivery, DynamicNodeProperties
from proton.handlers import Handler, MessagingHandler

from gofer.common import utf8
from gofer.messaging.adapter.reliability import YEAR


class Condition(object):
    """
    Base wait condition.
    """
    pass

    def __call__(self):
        """
        Test the condition.
        :return: True if condition is satisfied.
        :rtype: bool
        """
        raise NotImplementedError()


class ConnectionOpened(Condition):
    """
    Condition used to wait for the remote endpoint to be initialised.
    :ivar connection: The connection being opened.
    :type connection: proton.Connection
    """
    
    DESCRIPTION = 'connection opened'

    def __init__(self, connection):
        """
        :param connection: The connection being opened.
        :type connection: proton.Connection
        """
        super(ConnectionOpened, self).__init__()
        self.connection = connection

    def __call__(self):
        """
        Test the remote endpoint is initialized.
        :return: True if initialized.
        :rtype: bool
        """
        return not (self.connection.state & Endpoint.REMOTE_UNINIT)

    def __str__(self):
        return self.DESCRIPTION


class ConnectionClosed(Condition):
    """
    Condition used to wait for the remote endpoint to be inactive.
    :ivar connection: The connection being opened.
    :type connection: proton.Connection
    """
    
    DESCRIPTION = 'connection closed'

    def __init__(self, connection):
        """
        :param connection: The connection being closed.
        :type connection: proton.Connection
        """
        super(ConnectionClosed, self).__init__()
        self.connection = connection

    def __call__(self):
        """
        Test the remote endpoint is no longer active.
        :return: True if deactivated.
        :rtype: bool
        """
        return not (self.connection.state & Endpoint.REMOTE_ACTIVE)

    def __str__(self):
        return self.DESCRIPTION


class LinkCondition(Condition):
    """
    Condition used to wait for link changes.
    :ivar link: The link involved.
    :type link: proton.Link
    """

    def __init__(self, link):
        """
        :param link: The link involved.
        :type link: proton.Link
        """
        super(LinkCondition, self).__init__()
        self.link = link

    @property
    def name(self):
        """
        The link name.
        :rtype: str
        """
        return self.link.name

    @property
    def address(self):
        """
        The relevant link address.
        :rtype: str
        """
        if self.link.is_sender:
            return self.link.target.address
        else:
            return self.link.source.address

    def __call__(self):
        """
        Test the condition.
        :return: True if condition is satisfied.
        :rtype: bool
        """
        raise NotImplementedError()


class LinkAttached(LinkCondition):
    """
    Condition used to wait for link attached.
    """

    DESCRIPTION = 'link attached: %s/%s'

    def __call__(self):
        """
        Test the remote endpoint has been initialized.
        :return: True if initialized.
        :rtype: bool
        """
        return not (self.link.state & Endpoint.REMOTE_UNINIT)

    def __str__(self):
        return self.DESCRIPTION % (self.name, self.address)


class LinkDetached(LinkCondition):
    """
    Condition used to wait for link detached.
    """
    
    DESCRIPTION = 'link detached: %s'

    def __call__(self):
        """
        Test the remote endpoint has been deactivated.
        :return: True if deactivated.
        :rtype: bool
        """
        return not (self.link.state & Endpoint.REMOTE_ACTIVE)

    def __str__(self):
        return self.DESCRIPTION % self.link.name


class HasMessage(Condition):
    """
    Condition used to wait for the receiver handler has fetched
    at least one message.
    :ivar receiver: A receiver.
    :type receiver: Receiver
    """
    
    DESCRIPTION = 'fetch: %s/%s'

    def __init__(self, receiver):
        """
        :param receiver: A receiver.
        :type receiver: Receiver
        """
        super(HasMessage, self).__init__()
        self.receiver = receiver

    @property
    def name(self):
        """
        The link name.
        :rtype: str
        """
        return self.link.name

    @property
    def address(self):
        """
        The link address.
        :rtype: str
        """
        return self.link.source.address

    @property
    def link(self):
        """
        The link.
        :rtype: proton.Link
        """
        return self.receiver.link

    def __call__(self):
        """
        Test the receiver handler has at least one message.
        :return: True if has message.
        :rtype: bool
        """
        return self.receiver.handler.has_message()

    def __str__(self):
        return self.DESCRIPTION % (self.name, self.address)


class DeliverySettled(Condition):
    """
    Condition used to wait for a delivery to be settled.
    :ivar link: The link involved.
    :type link: proton.Link
    :ivar delivery: A message delivery.
    :type delivery: proton.reactor.Delivery
    """
    
    DESCRIPTION = 'delivery settled: %s'

    def __init__(self, link, delivery):
        """
        :param link: The link involved.
        :type link: proton.Link
        :ivar delivery: A message delivery.
        :type delivery: proton.reactor.Delivery
        """
        super(DeliverySettled, self).__init__()
        self.link = link
        self.delivery = delivery

    def __call__(self):
        """
        Test the delivery has been settled.
        :return: True if settled.
        :rtype: bool
        """
        return self.delivery.settled

    def __str__(self):
        return self.DESCRIPTION % self.link.name


class ConnectionError(ConnectionException):
    """
    A connection error condition.
    :ivar connection: The connection with an error.
    :type connection: Connection
    """

    DESCRIPTION = 'connection: %s failed: %s'

    @property
    def url(self):
        """
        The connection URL.
        :rtype: str
        """
        return self.connection.url

    @property
    def reason(self):
        """
        The cause of the error condition.
        :rtype: str
        """
        return self.connection.impl.remote_condition or 'by peer'

    def __init__(self, connection):
        """
        :param connection: The connection with an error.
        :type connection: Connection
        """
        super(ConnectionError, self).__init__(connection.url)
        self.connection = connection

    def __str__(self):
        return self.DESCRIPTION % (self.url, self.reason)


class LinkError(LinkException):
    """
    A link error condition.
    :ivar link: The link with an error.
    :type link: proton.Link
    """

    DESCRIPTION = 'link: %s/%s failed: %s'

    def __init__(self, link):
        """
        :param link: The link with an error.
        :type link: proton.Link
        """
        super(LinkError, self).__init__(link.name)
        self.link = link

    @property
    def name(self):
        """
        The link name.
        :rtype: str
        """
        return self.link.name

    @property
    def address(self):
        """
        The relevant link address.
        :rtype: str
        """
        if self.link.is_sender:
            return self.link.target.address
        else:
            return self.link.source.address

    @property
    def reason(self):
        """
        The reason for the link error.
        :rtype: str
        """
        return self.link.remote_condition or 'by peer'

    def __str__(self):
        return self.DESCRIPTION % (self.name, self.address, self.reason)


class DeliveryError(ProtonException):
    """
    A message delivery error.
    :ivar delivery: The failed delivery.
    :type delivery: proton.Delivery
    """

    def __init__(self, delivery):
        """
        :param delivery: The failed delivery.
        :type delivery: proton.Delivery
        """
        super(DeliveryError, self).__init__(delivery.state)
        self.delivery = delivery


class Messenger(object):
    """
    Provides message send/receive operations.
    :ivar connection: An open connection.
    :type connection: Connection
    :ivar link: An attached link.
    :type link: proton.Link
    """

    def __init__(self, connection, link):
        """
        :param connection: An open connection.
        :type connection: Connection
        :param link: An attached link.
        :type link: proton.Link
        """
        self.connection = connection
        self.link = link
        self.connection.wait(LinkAttached(link))
        self.detect_closed()

    def detect_closed(self):
        """
        Detect that the remote endpoint has been closed.
        :raise LinkError on detection.
        """
        if self.link.state & Endpoint.REMOTE_CLOSED:
            self.link.close()
            raise LinkError(self.link)


class Sender(Messenger):
    """
    A blocking message sender.
    """

    def send(self, message, timeout=None):
        """
        Send the specified message.
        :param message: The message to send.
        :type message: proton.Message
        :param timeout: The seconds to wait for the delivery to be settled.
        :type timeout: int
        :raise DeliveryError: when rejected or released.
        """
        delivery = self.link.send(message)
        condition = DeliverySettled(self.link, delivery)
        self.connection.wait(condition, timeout)
        if delivery.remote_state in [Delivery.REJECTED, Delivery.RELEASED]:
            raise DeliveryError(delivery)


class ReceiverHandler(MessagingHandler):
    """
    A message handler for the receiver.
    :ivar connection: An open connection.
    :type connection: Connection
    :ivar inbound: The fetched message queue.
    :type inbound: deque
    """

    def __init__(self, connection, prefetch=1):
        """
        :param connection: An open connection.
        :type connection: Connection
        :param prefetch: The number of messages to prefetch.
        :type prefetch: int
        """
        super(ReceiverHandler, self).__init__(prefetch, auto_accept=False)
        self.connection = connection
        self.inbound = deque()

    @property
    def container(self):
        """
        The reactor container.
        :rtype: proton.reactor.Container
        """
        return self.connection.container

    def pop(self):
        """
        Pop the next pre-fetched message from the internal queue.
        :return: The next message (or None)
            tuple of: (proton.Message, proton.Delivery)
        :rtype: tuple
        """
        return self.inbound.popleft()

    def has_message(self):
        """
        Test if the handler has pre-fetched messages.
        :return: True if not empty
        :rtype: bool
        """
        return len(self.inbound)

    def on_message(self, event):
        """
        Called by the container when a message is fetched.
        The message is stored in the local queue as:
            tuple of: (proton.Message, proton.Delivery)
        :param event: A proton (message delivery) event.
        :type event: proton.Event
        """
        self.inbound.append((event.message, event.delivery))
        self.container.yield_()

    def on_connection_error(self, event):
        """
        Called by the container when a connection error has occurred.
        :param event: The proton error event.
        :type event: proton.Event
        :raise ConnectionError:
        """
        raise ConnectionError(self.connection)

    def on_link_error(self, event):
        """
        Called by the container when a link error has occurred.
        The link is closed.
        :param event: The proton error event.
        :type event: proton.Event
        :raise LinkError:
        """
        if event.link.state & Endpoint.LOCAL_ACTIVE:
            event.link.close()
            raise LinkError(event.link)


class Receiver(Messenger):
    """
    A blocking message receiver.
    :ivar handler: A message handler.
    :type handler: ReceiverHandler
    :ivar credit: The number of flow control credits.
    :type credit: int
    """

    def __init__(self, connection, impl, handler, credit=1):
        """
        :param handler: A message handler.
        :type handler: ReceiverHandler
        :param credit: The number of flow control credits.
        :type credit: int
        """
        super(Receiver, self).__init__(connection, impl)
        self.handler = handler
        self.credit = credit
        self.flow()

    def flow(self):
        """
        Request the link perform flow control.
        """
        if not self.link.credit:
            self.link.flow(self.credit)

    def get(self, timeout=None):
        """
        Get the next available message.
        :param timeout: The seconds to wait for a message.
        :type timeout: int
        :return: The next message (or None)
            tuple of: (proton.Message, proton.Delivery)
        :rtype: tuple
        """
        self.flow()
        condition = HasMessage(self)
        self.connection.wait(condition, timeout)
        return self.handler.pop()

    def accept(self, delivery):
        """
        Update the delivery state to ACCEPTED and settle.
        :param delivery: A message delivery object.
        :type delivery: proton.Delivery
        """
        delivery.update(Delivery.ACCEPTED)
        delivery.settle()
        self.connection.process()

    def reject(self, delivery):
        """
        Update the delivery state to REJECTED and settle.
        :param delivery: A message delivery object.
        :type delivery: proton.Delivery
        """
        delivery.update(Delivery.REJECTED)
        delivery.settle()
        self.connection.process()


class Connection(Handler):
    """
    A blocking connection.
    :ivar url: The connection URL.
    :type url: basestring
    :ivar container: The reactor container.
    :type container: proton.reactor.Container
    :ivar impl: The actual proton connection object.
    :type impl: proton.Connection
    """

    def __init__(self, url):
        """
        :param url: The connection URL.
        :type url: basestring
        """
        self.url = utf8(url)
        self.container = Container()
        self.container.start()
        self.impl = None

    def is_open(self):
        """
        Get whether the connection has been opened.
        :return: True if open.
        :rtype bool
        """
        return self.impl is not None

    def open(self, timeout=None, ssl_domain=None, heartbeat=None):
        """
        Open the connection.
        :param timeout: The seconds to wait for the connection to be established.
        :type timeout: int
        :param ssl_domain: The proton SSL settings.
        :type ssl_domain: proton.SSLDomain
        :param heartbeat: The seconds between AMQP heartbeats. (None=disabled)
        :type heartbeat: int
        """
        if self.is_open():
            return
        url = Url(self.url)
        url.defaults()
        impl = self.container.connect(
            url=url,
            handler=self,
            ssl_domain=ssl_domain,
            heartbeat=heartbeat,
            reconnect=False)
        condition = ConnectionOpened(impl)
        self.wait(condition, timeout)
        self.impl = impl

    def process(self, timeout=0):
        """
        Request the container process the AMQP protocol.
        :param timeout: The container timeout.
        :type timeout: int
        """
        self.container.timeout = timeout
        self.container.process()

    def wait(self, condition, timeout=None):
        """
        Wait for the specified condition to be True.
        Call Connection.process()
        :param condition: The condition to wait for.
        :type condition: Condition
        :param timeout: The seconds to wait.
        :type timeout: int
        :raise proton.Timeout: when timeout exceeded.
        """
        remaining = timeout or YEAR
        while not condition():
            started = time()
            self.process(remaining)
            elapsed = time() - started
            remaining -= elapsed
            if remaining <= 0:
                raise Timeout(str(condition))

    def close(self):
        """
        Close the connection.
        """
        if self.is_open():
            return
        try:
            self.impl.close()
            condition = ConnectionClosed(self.impl)
            self.wait(condition)
        finally:
            self.impl = None

    def sender(self, address):
        """
        Create a blocking sender used for sending messages.
        :param address: An AMQP address.
        :type address: basestring
        :return: The configured sender.
        :rtype: Sender
        """
        name = str(uuid4())
        sender = self.container.create_sender(self.impl, utf8(address), name=name)
        return Sender(self, sender)

    def receiver(self, address, dynamic=False, credit=1):
        """
        Create a blocking receiver used for receiving AMQP address.
        :param address: An AMQP address.
        :type address: basestring
        :param dynamic: Indicates the address is dynamically assigned.
        :type dynamic: bool
        :param credit: The number of flow control credits.
        :type credit: int
        :return: The configured receiver.
        :rtype: Receiver
        """
        options = None
        name = str(uuid4())
        handler = ReceiverHandler(self, credit)
        if dynamic:
            # needed by dispatch router
            options = DynamicNodeProperties({'x-opt-qd.address': utf8(address)})
            address = None
        receiver = self.container.create_receiver(
            context=self.impl,
            source=utf8(address),
            name=name,
            dynamic=dynamic,
            handler=handler,
            options=options)
        return Receiver(self, receiver, handler, credit)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self):
        self.close()


# -------------- TEST --------------------------------------------------------

from proton import Message


URL = 'amqp://localhost'
ADDRESS = 'jeff'


def send(connection, n=10, timeout=5):
    print 'send()'
    sender = connection.sender(ADDRESS)
    while n > 0:
        message = Message(body='hello: %d' % n)
        sender.send(message, timeout=timeout)
        print 'sent: %d' % n
        n -= 1


def receive(connection, n=10, timeout=5):
    print 'receive()'
    receiver = connection.receiver(ADDRESS)
    while n > 0:
        m, d = receiver.get(timeout=timeout)
        print 'message: %s' % m.body
        n -= 1
        receiver.accept(d)


if __name__ == '__main__':
    connection = Connection(URL)
    connection.open(timeout=5)
    print 'opened'
    send(connection)
    receive(connection)
    connection.close()