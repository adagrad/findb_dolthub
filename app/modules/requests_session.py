import logging

import requests
import requests_random_user_agent

log = logging.getLogger(__name__)


class RequestsSession(object):

    def __init__(self, start_url=None, tor_socks_port=None, tor_control_port=None, tor_control_password="password") -> None:
        super().__init__()
        self.rsession = None
        self.start_url = start_url
        self.tor_socks_port = tor_socks_port
        self.tor_control_port = tor_control_port
        self.tor_control_password = tor_control_password
        self.reset_session()
        requests_random_user_agent_version = requests_random_user_agent.__version__

    def reset_session(self):
        log.info(f"reset requests session, using tor = {self.tor_socks_port is not None}")
        self.rsession = requests.Session() if self.tor_socks_port is None else self._get_tor_session()
        if self.start_url is not None:
            self.rsession.get(self.start_url)

    def _get_tor_session(self):
        session = requests.session()
        session.proxies = {
            'http':  f'socks5://127.0.0.1:{self.tor_socks_port}',
            'https': f'socks5://127.0.0.1:{self.tor_socks_port}'
        }

        # also get a new exit IP if control port is enabled
        if self.tor_control_port is not None:
            log.info("  .. ask for new exit ip")
            from stem import Signal
            from stem.control import Controller

            with Controller.from_port(port=self.tor_control_port) as controller:
                controller.authenticate(password=self.tor_control_password)
                controller.signal(Signal.NEWNYM)

        return session

