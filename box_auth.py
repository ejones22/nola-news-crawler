"""
Authenticate with Box to get refresh and access tokens, written to .env.
Run this once to get your tokens.
"""
import os
from flask import Flask, request, redirect
from dotenv import load_dotenv
import requests

load_dotenv()
app = Flask(__name__)

CLIENT_ID     = os.getenv("BOX_CLIENT_ID")
CLIENT_SECRET = os.getenv("BOX_CLIENT_SECRET")
REDIRECT_URI  = "http://127.0.0.1:5001/callback"

AUTH_URL  = "https://account.box.com/api/oauth2/authorize"
TOKEN_URL = "https://api.box.com/oauth2/token"

@app.route('/')
def index():
    print("\n" + "="*60)
    print("🔐 Starting Box Authentication Flow")
    print("="*60 + "\n")
    
    # Step 1: redirect user to Box's consent page
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
    }
    url = AUTH_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items())
    return redirect(url)

@app.route('/callback')
def callback():
    # Box redirects here with ?code=AUTH_CODE
    code = request.args.get("code")
    
    if not code:
        return "❌ Error: No authorization code received. Please try again."
    
    print(f"✅ Received authorization code: {code[:20]}...")
    
    data = {
        "grant_type":    "authorization_code",
        "code":          code,
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri":  REDIRECT_URI,
    }
    
    # Step 2: exchange for tokens
    print("🔄 Exchanging code for tokens...")
    resp = requests.post(TOKEN_URL, data=data)
    
    if resp.status_code != 200:
        error_msg = f"❌ Error getting tokens: {resp.status_code} - {resp.text}"
        print(error_msg)
        return error_msg
    
    tokens = resp.json()
    access_token  = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    
    if not access_token or not refresh_token:
        return "❌ Error: Tokens not found in response"
    
    print("✅ Successfully received tokens!")
    
    # Step 3: Persist tokens back into .env
    print("💾 Saving tokens to .env file...")
    
    # Read existing .env content
    env_lines = []
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            env_lines = f.readlines()
    
    # Remove old token lines if they exist
    env_lines = [line for line in env_lines 
                 if not line.startswith("BOX_ACCESS_TOKEN=") 
                 and not line.startswith("BOX_REFRESH_TOKEN=")]
    
    # Add new tokens
    with open(".env", "w") as f:
        f.writelines(env_lines)
        f.write(f"\nBOX_ACCESS_TOKEN={access_token}\n")
        f.write(f"BOX_REFRESH_TOKEN={refresh_token}\n")
    
    print("\n" + "="*60)
    print("✅ SUCCESS! Tokens saved to .env file")
    print("="*60)
    print("\nYour .env file now contains:")
    print("  - BOX_CLIENT_ID")
    print("  - BOX_CLIENT_SECRET")
    print("  - BOX_ACCESS_TOKEN")
    print("  - BOX_REFRESH_TOKEN")
    print("\n⚠️  Keep this file secure and never commit it to git!")
    print("="*60 + "\n")
    
    return """
    <html>
    <head>
        <style>
            body {
                font-family: Arial, sans-serif;
                max-width: 600px;
                margin: 50px auto;
                padding: 20px;
                background: #f5f5f5;
            }
            .success-box {
                background: #4CAF50;
                color: white;
                padding: 20px;
                border-radius: 5px;
                text-align: center;
            }
            .info-box {
                background: white;
                padding: 20px;
                margin-top: 20px;
                border-radius: 5px;
                border-left: 4px solid #2196F3;
            }
        </style>
    </head>
    <body>
        <div class="success-box">
            <h1>✅ Authentication Successful!</h1>
            <p>Your tokens have been saved to the .env file</p>
        </div>
        <div class="info-box">
            <h3>Next Steps:</h3>
            <ol>
                <li>You can close this browser window</li>
                <li>Stop the Flask server (Ctrl+C in terminal)</li>
                <li>Check your .env file - it now has all the tokens</li>
                <li>You're ready to run your Box crawler!</li>
            </ol>
        </div>
    </body>
    </html>
    """
    
if __name__ == '__main__':
    print("\n" + "="*60)
    print("🚀 Box Authentication Server Starting")
    print("="*60)
    print("\n📋 Instructions:")
    print("   1. Open your browser to: http://127.0.0.1:5001/")
    print("   2. You'll be redirected to Box to authorize")
    print("   3. Click 'Grant access to Box'")
    print("   4. Tokens will be automatically saved to .env")
    print("\n⚠️  Make sure your .env file has BOX_CLIENT_ID and BOX_CLIENT_SECRET")
    print("="*60 + "\n")
    
    app.run(host='127.0.0.1', port=5001, debug=True)