import os
from subprocess import Popen, PIPE
from novaclient.client import Client as novaClient


class OsUtils(object):
    def __init__(self, keystonerc_path):
        self._keystonerc_path = keystonerc_path
        self._creds = self.get_keystone_creds()
        assert (self._creds), 'Could not acquire OS credentials from environment'


    def get_keystone_creds(self):

        try:
            username = os.environ['OS_USERNAME']
            password = os.environ['OS_PASSWORD']
            auth_url = os.environ['OS_AUTH_URL']
            tenant_name = os.environ['OS_TENANT_NAME']

        except KeyError:
            # Couldn't load Openstack env vars.
            # Try to source keystonerc_admin and load it to memory.
            os_creds = self.source_keystonerc()
            if not os_creds:
                print "ENV missing some of OS_USERNAME, OS_PASSWORD, OS_AUTH_URL, OS_TENANT_NAME"
                return False
            else:
                username = os_creds['OS_USERNAME']
                password = os_creds['OS_PASSWORD']
                auth_url = os_creds['OS_AUTH_URL']
                tenant_name = os_creds['OS_TENANT_NAME']

        creds1 = {
            'username': username,
            'password': password,
            'auth_url': auth_url,
            'tenant_name': tenant_name
            }

        creds2 = {
            'username': username,
            'api_key': password,
            'auth_url': auth_url,
            'project_id': tenant_name
            }

        creds3 = {
            'username': username,
            'api_key': password,
            'auth_url': auth_url,
            'project_name': tenant_name,
            'mistral_url': 'http://10.1.20.5:8989/v2'
           }
        return {'creds1': creds1,
                'creds2': creds2,
                'creds3': creds3}


    def source_keystonerc(self):

        pipe = Popen("source %s; env | grep '=' | grep OS" % self._keystonerc_path, stdout=PIPE, shell=True)
        data = pipe.communicate()[0]

        os_creds = {}
        for line in data.splitlines():

            key, value = line.split("=")

            if key == "OS_USERNAME":
                os_creds[key] = value
            elif key == "OS_PASSWORD":
                os_creds[key] = value
            elif key == "OS_AUTH_URL":
                os_creds[key] = value
            elif key == "OS_TENANT_NAME":
                os_creds[key] = value

        if not os_creds["OS_USERNAME"] or not os_creds["OS_PASSWORD"] \
                or not os_creds["OS_AUTH_URL"] or not os_creds["OS_TENANT_NAME"]:
            return None

        return os_creds


class Nova(OsUtils):

    def __init__(self, keystonerc_path):
        super(Nova, self).__init__(keystonerc_path)
        self._client= novaClient('2', **self._creds['creds2'])
        self._aggregates = self._client.aggregates.list()
        self._hosts_dict_list = self.get_hosts_dict()

    def get_hosts_dict(self, service='compute'):
        _hosts = self._client.hosts
        _hosts_list = _hosts.list_all()
        return [_host.to_dict() for _host in _hosts_list]

    def get_host_by_service_type(self, service):
        hosts = []
        for _host in self.get_hosts_dict():
            if _host['service'] == service:
                hosts.append(_host)
        return hosts

    def verify_host_exists(self, host, service=u'compute'):
        for _h in self.get_hosts_dict():
            if _h['service'] == service and _h['host_name'] == host:
                break
        else:
            print 'Host %s not found for service %s' % (host, service)
            return False
        return True

    def verify_host_in_zone(self, host, zone, service=u'compute'):
        if not self.verify_host_exists(host):
            print "Host %s does not exist" % host
            return False
        if not self.verify_zone_exists(zone):
            print "A_Zone %s does not exist" % zone
            return False
        if zone not in self.get_host_zones(host):
            print "Host %s not in A_Zone %s" % (host, zone)
            return False
        return True

    def verify_zone_exists(self, zone):
        """
        Verify that aggregate with a_z exists.
        If one exists, returns the agg object
        :param zone: name of nova-zone
        :return: the aggregate instance or False if doesnt exist
        :rtype: novaclient.v2.aggregates.Aggregate
        """
        for x in self._aggregates:
            if x.availability_zone == zone:
                agg = x
                break
        else:
            print 'No such zone %s' % zone
            return False
        print "Found zone %s in host-agg %s" % (zone, agg.name)
        return agg

    def _get_services_dict(self):
        services_dict = [_ser.to_dict() for _ser in self._client.services.findall()]
        return services_dict

    def _find_service_id_by_host(self, host, binary='nova-compute'):
        services_dict = self._get_services_dict()
        for ser in services_dict:
            if ser['host'] == host and ser['binary'] == binary:
                return ser['id']
        else:
            return None

    def check_if_host_disabled(self, host, binary='nova-compute'):
        """
        Return True if a nova-compute service exists and in disabled mode
        :param host: hostname to check
        :return: Is host nova-compute service in disabled mode?
        :rtype: bool
        """
        _service_id = self._find_service_id_by_host(host, binary)
        if _service_id:
            _ser = self._client.services.find(id=_service_id)
            if _ser.status == u'enabled':
                return False
            elif _ser.status == u'disabled':
                return True
        else:
            return False

    def enable_host_service(self, host, binary='nova-compute', enable=True):
        """
        enable/disable nova-service
        :param host: hostname of service
        :param binary: type of nova service
        :param enable: enable/disable desvice
        :type enable: bool
        :return: Success
        :rtype: bool
        """
        _id = self._find_service_id_by_host(host, binary)
        _res = False
        if _id:
            _host_service = self._client.services.find(id=_id)
            _host_manager = _host_service.manager
            if enable:
                ser = _host_manager.enable(host, binary)
                if ser.status == u'enabled':
                    _res = True
            else:
                ser = _host_manager.disable(host, binary)
                if ser.status == u'disabled':
                    _res = True
        return _res


    def add_host_to_zone(self, host, zone):
        """
        Add a host to aggregate that belongs to av_zone
        :param host: hostname
        :param zone: dst availability zone
        :return: success/fail
        :rtype: bool
        """
        # Enable host nova service (in case disabled in the past)
        self.enable_host_service(host)

        # Verify host is registered.
        if not self.verify_host_exists(host):
            raise NovaNoSuchHost(host)
        # Verify a_z exists
        _agg = self.verify_zone_exists(zone)
        if not _agg:
            raise NovaNoSuchZone(zone)
        if self.get_host_zones(host) != ['nova']:
            for _zone in self.get_host_zones(host):
                self.remove_host_from_zone(host, _zone)
        _agg.add_host(host)
        # make sure success
        if zone in self.get_host_zones(host):
            return True
        else:
            return False

    def get_host_zones(self, host):
        """
        Get a list of availability zones to which the compute belongs.
        :param host:
        :return: list of zones to which compute belongs
        """
        _zones = [_host['zone'] for _host in self.get_hosts_dict()
                   if _host['host_name'] == host]
        return _zones

    def remove_host_from_zone(self, host, zone):
        """
        Remove a host from a specific zone (compute is added to the nova general zone).
        :param host:
        :param zone:
        :return:
        """
        _agg = self.verify_zone_exists(zone)
        if not _agg:
            raise NovaNoSuchZone(zone)
        if not self.verify_host_exists(host):
            raise NovaNoSuchHost(host)
        if not self.verify_host_in_zone(host, zone):
            raise NovaHostNotInZone(host, zone)
        _agg.remove_host(host)
        if not zone in self.get_host_zones(host):
            return True
        else:
            print"Seems like host %s was not added to zone %s" % (host, zone)
            return False

    def add_compute_to_host_aggregate(self, compute, host_aggregate):

        if not compute.endswith(".local"):
            compute = "".join((compute, ".local"))

        for agg in self._aggregates:
            if host_aggregate == agg.name:
                agg.add_host(compute)


###### Exceptions


class NovaNoSuchZone(Exception):
    def __init__(self, zone):
        self.message = "No such availability-zone %s" % zone


class NovaNoSuchHost(Exception):
    def __init__(self, host, service='compute'):
        self.message = "No such host %s for Service %s" % (host, service)


class NovaHostNotInZone(Exception):
    def __init__(self, host, zone):
        self.message = "Host %s Is not in A_Zone %s" % (host, zone)


class NovaHostAlreadyAssigned(Exception):
    def __init__(self, host, zones):
        self.message = "Host %s already member of A_Z %s" % (host, str(zones))
