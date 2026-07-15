# TODO (YOU write this) — containerize the fraud-detection API. This is a graded
# Unit 10 objective ("Containerization", 14 pts). Requirements:
#
#   - Multi-stage build: a builder stage that installs from requirements.txt,
#     and a slim runtime stage that copies only what it needs.
#   - Copy the application code (src/), plus models/ and data/.
#   - Run as a NON-ROOT user (security).
#   - Add a HEALTHCHECK that hits  GET /health  on port 8000.
#   - EXPOSE 8000 and start the API with:
#       uvicorn src.api.main:app --host 0.0.0.0 --port 8000
#
# Tips: base image python:3.11-slim; `pip install --prefix=/install` in the
# builder, then copy /install into the runtime stage. Test it with:
#   docker build -t fraud-api .
#   docker run --rm -p 8000:8000 fraud-api   # then curl http://localhost:8000/health
#
# (Kafka + Redis are already provided for you in docker-compose.yml — you only
#  need to containerize YOUR app and wire it in.)
