ARG CHROME_VERSION=142.0.7444.175-1
ARG GALLERY_DL_VERSION=v1.28.5

FROM oven/bun:1.2.8-slim AS base
FROM base AS builder

RUN apt-get update \
    # For prisma client dependencies
    && apt-get install -y openssl

WORKDIR /app
COPY package.json bun.lock tsconfig.json ./
COPY ./app/tweet-forwarder ./app/tweet-forwarder
COPY ./core ./core
COPY ./assets/fonts ./assets/fonts
COPY ./assets/branding ./assets/branding

ENV PUPPETEER_SKIP_DOWNLOAD true
ENV DATABASE_URL="file:/app/data.db"

RUN bun install --filter "./app/tweet-forwarder" --filter "./core/*" --frozen-lockfile
RUN bun --filter "@idol-bbq-utils/tweet-forwarder" db:generate-client
RUN bun --filter "@idol-bbq-utils/tweet-forwarder" build
RUN bun build app/tweet-forwarder/scripts/export-biliup-browser-cookies.ts --outdir=./app/tweet-forwarder/build --target=bun --minify

FROM base AS runner

ARG CHROME_VERSION
ARG GALLERY_DL_VERSION
RUN apt-get update \
    && apt-get install -y wget gnupg curl unzip ffmpeg xvfb dbus-x11 python3 python3-venv \
    # Required for Chrome
    && apt-get install -y fonts-ipafont-gothic fonts-ipafont-mincho fonts-liberation fonts-noto-cjk libasound2 libatk-bridge2.0-0 libatk1.0-0 libatspi2.0-0 libcairo2 libcups2 libcurl4 libdbus-1-3 libdrm2 libgbm1 libglib2.0-0 libgtk-3-0 libnspr4 libnss3 libpango-1.0-0 libu2f-udev libvulkan1 libx11-xcb1 libxcomposite1 libxdamage1 libxfixes3 libxkbcommon0 libxrandr2 libxshmfence1 libxss1 xdg-utils \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome
RUN wget --no-verbose -O /tmp/chrome.deb https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_${CHROME_VERSION}_amd64.deb \
    && apt install -y /tmp/chrome.deb \
    && rm /tmp/chrome.deb

# Install gallery-dl
RUN wget https://github.com/mikf/gallery-dl/releases/download/${GALLERY_DL_VERSION}/gallery-dl.bin -O /usr/local/bin/gallery-dl \
    && chmod +x /usr/local/bin/gallery-dl

WORKDIR /app
COPY app/tweet-forwarder/update-media-tools.sh /app/tools/update-media-tools.sh
COPY app/tweet-forwarder/scripts/biliup-upload.py /app/tools/biliup-upload.py
RUN chmod +x /app/tools/update-media-tools.sh \
    && chmod +x /app/tools/biliup-upload.py \
    && /app/tools/update-media-tools.sh
COPY --from=builder /app/app/tweet-forwarder/build/main.js /app/bin.js
COPY --from=builder /app/app/tweet-forwarder/build/export-biliup-browser-cookies.js /app/tools/export-biliup-browser-cookies.js
COPY --from=builder /app/app/tweet-forwarder/build/resvgjs* /app/
# Copy assets
COPY --from=builder /app/assets/fonts /app/assets/fonts
COPY --from=builder /app/assets/branding /app/assets/branding
# Copy node_modules (including prisma)
COPY --from=builder /app/node_modules /app/node_modules

# Copy source prisma schema and migrations for deployment
COPY app/tweet-forwarder/prisma /app/prisma
# Copy generated client last so it does not get overwritten by the checked-in prisma folder
COPY --from=builder /app/app/tweet-forwarder/prisma/client /app/prisma/client
# Copy startup script
COPY app/tweet-forwarder/start.sh /app/start.sh

RUN mkdir -p /tmp/tweet-forwarder \
    && chown -R bun:bun /tmp/tweet-forwarder

RUN chown -R bun:bun /app/ \
    && chmod +x /app/start.sh

USER bun

ENV NO_SANDBOX=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/google-chrome-stable
ENV DATABASE_URL="file:/app/data.db"
ENV FONTS_DIR=/app/assets/fonts
ENV ENABLE_XVFB=1
ENV XVFB_DISPLAY=:99
ENV XVFB_SCREEN="0 1600x1200x24"
ENV BROWSER_PROFILE_DIR=/app/assets/cookies/browser-profiles
ENV PATH="/app/tools/bin:${PATH}"

CMD ["/app/start.sh"]
