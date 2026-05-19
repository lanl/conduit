## Quick Start

This quick start guide will use the docker compose example found in the README. This example can be used as an example for a full conduit deployment within a containerized environment. The [Build script](https://github.com/lanl/conduit/blob/main/examples/docker/build.sh) generates all the required certs/keys for conduit to operate. The [run script](https://github.com/lanl/conduit/blob/main/examples/docker/run.sh) starts conduit with docker compose and opens an interactive session to a client container.

### Dependencies

- [docker](https://docs.docker.com/engine/install/)
- [docker-compose](https://docs.docker.com/compose/install/)
- [git](https://git-scm.com/)

### How to use

1.  Clone the repository:

            git clone https://github.com/lanl/conduit
            cd conduit

2.  Navigate to `conduit/examples/docker` and run `build.sh`. This will build all the necessary docker images and setup the necessary configuration files and keys into /etc/conduit:

        # create the conduit config area (this may require root privileges)
        mkdir /etc/conduit
        cd conduit/examples/docker
        ./build.sh

3.  Start the services by running the `run.sh` script. This end with a bash terminal that's running in the `client` container:

        ./run.sh

4.  Run an example transfer

        # Start a conduit transfer
        conduit cp /mnt/fs_1/foo/hello.txt /mnt/fs_2/bar/
        # Get status on all conduit transfers
        conduit status

Notes:

- kinit is run when the `client` container is run. If you need to get a new ticket, run any kerberos commands for `testuser`. The password is `password`

       # kinit as testuser to get a kerberos ticket
       kinit testuser
       # enter the password for testuser which is 'password'
       Password for testuser@example.com: password
       # view kerberos ticket
       klist

- the destroy script can be used to cleanup all the files created by this example:

       conduit/conduit/examples/docker/destroy.sh
