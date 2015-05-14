# Forklift - Docker Container

### Steps to generate docker container

#### 1. Create local distribution copy of the forklift server.

Call the sbt clean compile distribution commands. This will create a zip file that we are going to put inside the docker container.

```
$ sbt clean compile dist
```
#### 2. Build docker image

Start up docker and ```cd``` to the root of your forklift project. ```ls``` to make sure see the Dockerfile in this directory. Call the docker build command. Use ```-t``` to tag it.

```
$ sudo docker build -t forklift-server:0.1 .
```
You see your newly created image by calling
```
$ sudo docker images
```
You will see a list of your docker images

```
REPOSITORY          TAG                 IMAGE ID            CREATED             VIRTUAL SIZE
forklift-server     0.1                 930b358d9b42        47 seconds ago      827.2 MB
dockerfile/java     oracle-java8        197036717ddd        2 days ago          750.7 MB
```

#### 3. Run the docker container

now that the container is built, we need to run it.
```
$ sudo docker run forklift-server:0.1
```
check to see if it ran
```
$ sudo docker ps -a
```
You should see something like this:
```
CONTAINER ID        IMAGE                 COMMAND             CREATED             STATUS                     PORTS               NAMES
baff4bd2f2b6        forklift-server:0.1   "bash"              8 seconds ago       Exited (0) 7 seconds ago                       angry_shockley
```
#### 4. Start forklift inside the docker container

Now that we know it runs, lets run an interactive docker container so we can drop in the shell of the containr and fire up forklift. 

We will be using the ```-i``` flag to make it interactive and the ```-t``` flag to create a tty. You can chain these 2 flags together.
```
$ sudo docker run -it forklift-server:0.1 bash
```
We should now get a prompted and be in the ```/usr/local/forklift-server-0.1``` folder.

Now we can start forklift
```
forklift-server tcp://{your.activemq.ip.address:port} /usr/local/forklift/consumers
```

#### 4. Testing / Helpful tips

When you are testing consumers for forklift below is a helpful script you can use that will link a local folder to the consumers folder inside the docker container.

syntax:
```
$ sudo docker run -i -t - ~local-folder:docker-container-folder container-tag
```

forklift example

```
$ sudo docker run -i -t -v ~/Desktop/consumers:/usr/local/forklift/consumers forklift-server:0.1 /bin/bash
```

#### Troubleshooting

If you are running into problems during the build process or your docker image is not updating consider some of the following suggestions

* Delete your local distribution of forklift and the target folders, then ```sbt clean compile dist```
* Delete your forklift docker containers ```$ sudo docker rm <docker-container-id>```
* Delete your forklift docker images ```$ sudo docker rmi <docker-image-id>```