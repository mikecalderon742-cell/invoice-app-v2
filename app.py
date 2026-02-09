from flask import Flask, render_template, request

app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def home():
    if request.method == "POST":
        client = request.form.get("client")
        amount = request.form.get("amount")

        return f"""
        <h1>Invoice Preview</h1>
        <p><strong>Client:</strong> {client}</p>
        <p><strong>Amount:</strong> ${amount}</p>
        <a href="/">Create another invoice</a>
        """

    return render_template("index.html")

@app.route("/health")
def health():
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)