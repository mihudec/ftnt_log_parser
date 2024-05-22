import pathlib
import ipaddress
import pandas as pd

BASE_DB_DIR = pathlib.Path.home().joinpath("Develop/ipinfo/db")

DB_PATH = BASE_DB_DIR.joinpath("country_asn.csv.gz")

def convert_to_ipaddress(ip):
    try:
        return ipaddress.ip_address(ip)
    except ValueError:
        return None  # Return None for invalid IPs
    
def get_ip_version(ip):
    if ip is not None:
        if isinstance(ip, ipaddress.IPv4Address):
            return 'IPv4'
        elif isinstance(ip, ipaddress.IPv6Address):
            return 'IPv6'
    return None

def load_db():
    df = pd.read_csv(DB_PATH, compression='gzip')
    df['start_ip'] = df['start_ip'].apply(convert_to_ipaddress)
    df['end_ip'] = df['end_ip'].apply(convert_to_ipaddress)
    df['ip_version'] = df.apply(lambda row: get_ip_version(row['start_ip']), axis=1)
    df_ipv4 = df[df['ip_version'] == 'IPv4']
    df_ipv6 = df[df['ip_version'] == 'IPv6']
    return df_ipv4, df_ipv6


def find_row_for_ip(df, ip):
    ip = ipaddress.ip_address(ip)
    for index, row in df.iterrows():
        if row['start_ip'] <= ip <= row['end_ip']:
            return row
    return None  # Return None if no row contains the IP address



def main():
    df_ipv4, df_ipv6 = load_db()
    ip = ipaddress.IPv4Address("62.109.150.0")
    row = find_row_for_ip(df_ipv4, ip)
    print(row)


if __name__ == '__main__':
    main()