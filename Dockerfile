FROM denoland/deno:alpine-1.16.2

USER deno

WORKDIR /app

COPY main.ts /app/main.ts
RUN deno cache main.ts

ENTRYPOINT ["deno", "run", "--allow-net", "/app/main.ts"]
