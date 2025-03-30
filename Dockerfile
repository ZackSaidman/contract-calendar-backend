FROM public.ecr.aws/lambda/python:3.12

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

RUN pip install dateparser
# Copy function code
COPY src/ ${LAMBDA_TASK_ROOT}

# Set the Lambda function handler
CMD ["app.lambda_handler"]