events {}

http {
    # Blue-green upstream. Exactly ONE server line is active; the other is
    # commented out. switch_traffic.sh flips these two lines and reloads nginx,
    # so in-flight requests drain and new requests go to the new version.
    upstream fraud_backend {
        server api-blue:8000;      # ACTIVE
        # server api-green:8000;   # STANDBY
    }

    server {
        listen 80;
        location / {
            proxy_pass http://fraud_backend;
            proxy_set_header Host $host;
        }
    }
}
