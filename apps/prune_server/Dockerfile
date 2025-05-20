# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY ./main.py /app/

# Install any needed packages specified in requirements.txt
# For this simple app, we install fastapi and uvicorn directly
RUN pip install uv

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Run main.py when the container launches
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
CMD ["./main.py", "--host", "0.0.0.0", "--port", "8000"]