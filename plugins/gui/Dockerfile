FROM build.sofi.com:5000/sofi-node-base

# Add source
ADD . /noise

WORKDIR /noise

# Expose the default node hosting port.
EXPOSE 3000

RUN chmod 755 /noise/bin/run
ENTRYPOINT ["/noise/bin/run"]
