# 1. BASE SYSTEM: Use the official, lightweight Python 3.11 image
FROM python:3.11-slim

# 2. SYSTEM DEPENDENCIES: Install essential Linux build tools for libraries like ephem
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 3. WORKSPACE: Set the secure directory inside the container
WORKDIR /app

# 4. DEPENDENCIES: Copy requirements and install them securely (no-cache prevents memory bloat)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. SOURCE CODE: Copy the rest of your proprietary code into the container
COPY . .

# 6. PORT EXPOSURE: Open the standard Streamlit port
EXPOSE 8501

# 7. HEALTHCHECK: Tells DigitalOcean if the app crashes so it can auto-restart it
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# 8. SECURE IGNITION: Use Python to perfectly preserve vault quotes and newlines, then launch
CMD sh -c "mkdir -p .streamlit && python -c \"import os; open('.streamlit/secrets.toml', 'w').write(os.environ.get('SECRETS_TOML', ''))\" && streamlit run app.py --server.port=8501 --server.address=0.0.0.0"
