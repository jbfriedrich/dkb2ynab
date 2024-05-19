# Use an official Python runtime as a parent image
FROM python:3

# Upgrade pip
RUN pip install --upgrade pip

# Create a user in the docker container
RUN adduser dkb2ynab
USER dkb2ynab

# Set the working directory
WORKDIR /home/dkb2ynab

# Install pipenv and extend PATH
#RUN pip install --user pipenv
ENV PATH="/home/dkb2ynab/.local/bin:${PATH}"

# Copyring required files to the workdir
COPY --chown=dkb2ynab:dkb2ynab dkb2ynab.py dkb2ynab.py
COPY --chown=dkb2ynab:dkb2ynab requirements.txt requirements.txt

# Installing and locking requirements
#RUN pipenv install
#RUN pipenv lock
RUN pip install --user -r requirements.txt

# Run the script
CMD ["python", "dkb2ynab.py"]
