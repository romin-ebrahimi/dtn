# DTN
Real time streaming of exchange data through 
[DTN / IQFeed](https://www.iqfeed.net/).

### Info
Given that the [IQFeed API](https://www.iqfeed.net/) is only available on 
Windows, an installation of [Wine](https://www.winehq.org/) is necessary for the 
API to work with a Linux Docker image. After the IQFeed API is launched, any 
Kafka producers can connect to the API and begin streaming data.

- `docker_iqfeed/Dockerfile` creates the Docker image that has IQFeed installed. 
The pre-built image is uploaded to the Docker hub to speed up deployment. This 
image can be re-used when a new version of IQFeed is released.
- `iqfeed` contains the classes and methods for connecting to the IQFeed API,
maintaining connection health checks, and relaying messages. You will need to
add your DTN login credentials to use the service.py class to connect to IQFeed.