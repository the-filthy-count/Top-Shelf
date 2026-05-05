FROM python:3.12-slim

# Install ffmpeg + ImageMagick (with librsvg for SVG → PNG rendering used by
# the studio-logo normaliser).
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    imagemagick \
    librsvg2-bin \
    && rm -rf /var/lib/apt/lists/*

# Allow ImageMagick to read SVG — Debian's default policy.xml disables it
# for security; we only ever feed it studio logo bytes from trusted sources
# (StashDB / TPDB / our seed archive). Targets both ImageMagick 6 and 7
# policy paths so the build works on either base image.
RUN for pol in /etc/ImageMagick-6/policy.xml /etc/ImageMagick-7/policy.xml; do \
      if [ -f "$pol" ]; then \
        sed -i '/pattern="SVG"/s,rights="none",rights="read|write",' "$pol"; \
      fi; \
    done

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8891

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8891"]
