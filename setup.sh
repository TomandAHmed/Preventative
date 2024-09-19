#!/bin/bash

# Create the .streamlit directory if it doesn't exist
mkdir -p ~/.streamlit/

# Write the credentials.toml file
echo "\
[general]\n\
email = \"email@domain\"\n\
" > ~/.streamlit/credentials.toml

# Write the config.toml file with the PORT variable substituted correctly
echo "\
[server]\n\
headless = true\n\
enableCORS = false\n\
port = ${PORT}\n\
maxUploadSize = 500\n\
" > ~/.streamlit/config.toml
