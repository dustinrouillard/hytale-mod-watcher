FROM gcr.io/distroless/static:nonroot
LABEL org.opencontainers.image.source=https://github.com/dustinrouillard/hytale-mod-watcher

ENV PATH=/
ENV ENV=prod

COPY artifacts/hytale-mod-watcher /app

USER nonroot:nonroot
CMD ["app"]
