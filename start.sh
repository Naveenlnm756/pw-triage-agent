#!/bin/bash
# Start the background listener (unbuffered so logs show up instantly)
python -u -m src.gmail_listener &

# Start the Streamlit dashboard on the port provided by Railway
streamlit run admin_app.py --server.port $PORT --server.address 0.0.0.0
