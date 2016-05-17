#!/usr/bin/env python
from openstackutils import Nova as NovaUtils
import json
from pudb import set_trace

def main():

    # Get hosts list
    nova = NovaUtils("/opt/stack/keystonerc_admin")
    hosts = nova.get_host_by_service_type("compute")



if __name__ == "__main__":
    main()