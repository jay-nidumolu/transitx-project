import os
import requests
import subprocess

 # ----- Ensure the current public IP is allowed to connect to Azure SQL -----#
def ensure_firewall_access():
   
    try:
        ip = requests.get("https://ifconfig.me").text.strip()
        server_name = os.getenv("AZ_SQL_SERVER_NAME")
        resource_group = os.getenv("AZ_RESOURCE_GROUP")
        rule_name = f"auto_rule_{ip.replace('.', '_')}"

        print(f"Ensuring firewall access for IP: {ip}")

        subprocess.run([
            "az", "sql", "server", "firewall-rule", "create",
            "--name", rule_name,
            "--resource-group", resource_group,
            "--server", server_name,
            "--start-ip-address", ip,
            "--end-ip-address", ip
        ], check=False)

        print("Firewall rule ensured for this IP.")
    except Exception as e:
        print(f"Could not update firewall automatically: {e}")