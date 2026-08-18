# goreleaser injects the prebuilt binary for the target platform; this
# image is not intended for standalone docker build.
FROM gcr.io/distroless/static-debian12:nonroot
COPY driftless /usr/local/bin/driftless
EXPOSE 8724 8725
ENTRYPOINT ["/usr/local/bin/driftless"]
CMD ["serve"]
