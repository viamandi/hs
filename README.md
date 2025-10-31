Project makes a loopback of a message using MQTT broker to save the data into DB.

It contains docker yml to launch images for:
- download spring project and compiling it
- MQTT broker: HiveMQ
- Spring App application
- PostgreSQL to save the data from the message


Run:
  - Precondition:
    - install docker
  - docker-compose -up

File structure
  - project directory
  - docker setup is located in root project directory
