FROM python:3.10

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY index.html .

# Expose the port the app runs on
EXPOSE 8080

CMD ["python", "-m", "http.server", "8080"]