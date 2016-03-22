FROM node:5.6-onbuild

# Add source
ADD . /forklift-gui

WORKDIR /forklift-gui

# Expose the default node hosting port.
EXPOSE 3001

RUN chmod 755 /forklift-gui/bin/run
ENTRYPOINT ["/forklift-gui/bin/run"]
