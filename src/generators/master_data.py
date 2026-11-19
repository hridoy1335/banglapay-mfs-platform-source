
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from random import Random
from uuid import NAMESPACE_URL, uuid5


NAMESPACE = NAMESPACE_URL
SEED = 20261119

LOCATIONS = {
    "Dhaka": ["Dhanmondi", "Mirpur", "Savar", "Uttara"],
    "Gazipur": ["Gazipur Sadar", "Kaliakair", "Sreepur"],
    "Narayanganj": ["Narayanganj Sadar", "Rupganj", "Sonargaon"],
    "Chattogram": ["Chattogram Sadar", "Hathazari", "Patiya"],
    "Cumilla": ["Cumilla Sadar", "Daudkandi", "Debidwar"],
    "Sylhet": ["Sylhet Sadar", "Beanibazar", "Golapganj"],
    "Rajshahi": ["Rajshahi Sadar", "Paba", "Godagari"],
    "Khulna": ["Khulna Sadar", "Dumuria", "Batiaghata"],
    "Rangpur": ["Rangpur Sadar", "Mithapukur", "Badarganj"],
    "Barishal": ["Barishal Sadar", "Babuganj", "Bakerganj"],
    "Mymensingh": ["Mymensingh Sadar", "Trishal", "Muktagacha"],
    "Bogura": ["Bogura Sadar", "Sherpur", "Shajahanpur"],
    "Jashore": ["Jashore Sadar", "Jhikargacha", "Manirampur"],
    "Faridpur": ["Faridpur Sadar", "Bhanga", "Boalmari"],
    "Tangail": ["Tangail Sadar", "Mirzapur", "Ghatail"],
}

FIRST_NAMES = [
    "Rahim", "Karim", "Hasan", "Hossain", "Tanvir",
    "Sakib", "Nusrat", "Ayesha", "Sadia", "Sumaiya",
    "Farhan", "Imran", "Rafi", "Nabila", "Mim",
    "Arif", "Tamanna", "Mahmud", "Jannat", "Tariq",
]

LAST_NAMES = [
    "Ahmed", "Hossain", "Islam", "Rahman", "Khan",
    "Uddin", "Chowdhury", "Akter", "Begum", "Hasan",
]

MERCHANT_CATEGORIES = [
    "GROCERY",
    "RESTAURANT",
    "PHARMACY",
    "FASHION",
    "ELECTRONICS",
    "TRANSPORT",
    "SUPERMARKET",
    "ONLINE_RETAIL",
    "EDUCATION",
    "HEALTHCARE",
]

PROVIDERS = [
    ("BILLER", "Metro Electricity Services"),
    ("BILLER", "City Water Services"),
    ("BILLER", "Delta Gas Services"),
    ("BILLER", "Metro Internet Services"),
    ("BILLER", "Northern Power Services"),
    ("BILLER", "Urban Water Services"),
    ("BILLER", "Central Gas Services"),
    ("BILLER", "Digital Cable Services"),
    ("BILLER", "Regional Electricity Services"),
    ("BILLER", "City Utility Services"),
    ("MOBILE_OPERATOR", "SimTel One"),
    ("MOBILE_OPERATOR", "Nova Mobile"),
    ("MOBILE_OPERATOR", "Delta Connect"),
    ("CORPORATE", "BanglaPay Corporate 01"),
    ("CORPORATE", "BanglaPay Corporate 02"),
    ("CORPORATE", "BanglaPay Corporate 03"),
    ("CORPORATE", "BanglaPay Corporate 04"),
    ("CORPORATE", "BanglaPay Corporate 05"),
    ("GOVERNMENT", "Public Service Portal 01"),
    ("GOVERNMENT", "Public Service Portal 02"),
]


def stable_uuid(entity: str, reference: str) -> str:
    """Generate a repeatable UUID for an entity and business reference."""
    return str(
        uuid5(NAMESPACE, f"banglapay:{entity}:{reference}")
    )


def generate_master_data(
    customers_count: int = 500,
    agents_count: int = 20,
    outlets_per_agent: int = 2,
    merchants_count: int = 50,
    terminals_count: int = 75,
    devices_count: int = 400,
) -> dict:
    if min(
        customers_count,
        agents_count,
        outlets_per_agent,
        merchants_count,
        terminals_count,
        devices_count,
    ) < 0:
        raise ValueError("Record counts cannot be negative")

    if devices_count > customers_count:
        raise ValueError(
            "Initial seed supports at most one device per customer"
        )

    rng = Random(SEED)
    now = datetime.now(timezone.utc)

    districts = list(LOCATIONS.keys())

    customer_rows = []
    profile_rows = []
    wallet_rows = []
    balance_rows = []
    customer_ids = []

    # --------------------------------------------------
    # 1. Customers, profiles and wallets
    # --------------------------------------------------

    for i in range(1, customers_count + 1):
        ref = f"CUS{i:06d}"
        customer_id = stable_uuid("customer", ref)
        profile_id = stable_uuid("profile", ref)
        wallet_ref = f"WAL{i:06d}"
        wallet_id = stable_uuid("wallet", wallet_ref)

        first_name = rng.choice(FIRST_NAMES)
        last_name = rng.choice(LAST_NAMES)
        display_name = f"{first_name} {last_name}"

        district = rng.choice(districts)
        upazila = rng.choice(LOCATIONS[district])

        registered_at = now - timedelta(
            days=rng.randint(60, 900)
        )

        # 5% pending KYC, 95% active customers.
        pending_kyc = i % 20 == 0

        account_status = (
            "PENDING_KYC" if pending_kyc else "ACTIVE"
        )

        kyc_status = (
            "PENDING" if pending_kyc else "VERIFIED"
        )

        registration_channel = rng.choices(
            ["MOBILE_APP", "AGENT", "BRANCH", "API"],
            weights=[70, 20, 8, 2],
            k=1,
        )[0]

        customer_rows.append((
            customer_id,
            ref,
            "INDIVIDUAL",
            account_status,
            kyc_status,
            registration_channel,
            registered_at,
        ))

        profile_rows.append((
            profile_id,
            customer_id,
            display_name,
            f"SIM-BD-{i:08d}",
            f"customer{i:06d}@example.test",
            district,
            upazila,
            rng.choice(["bn", "en"]),
        ))

        wallet_rows.append((
            wallet_id,
            wallet_ref,
            customer_id,
            "PRIMARY",
            "PENDING" if pending_kyc else "ACTIVE",
            "BDT",
            registered_at + timedelta(hours=1),
        ))

        # Initial financial balances are zero.
        balance_rows.append((
            wallet_id,
            0,
            0,
            0,
            0,
        ))

        customer_ids.append(customer_id)

    # --------------------------------------------------
    # 2. Agents, outlets and float accounts
    # --------------------------------------------------

    agent_rows = []
    outlet_rows = []
    float_rows = []
    outlet_ids = []

    for i in range(1, agents_count + 1):
        ref = f"AGT{i:04d}"
        agent_id = stable_uuid("agent", ref)

        district = rng.choice(districts)
        upazila = rng.choice(LOCATIONS[district])

        agent_type = (
            "SUPER_AGENT" if i % 5 == 0
            else "RETAIL_AGENT"
        )

        business_type = rng.choice([
            "INDIVIDUAL",
            "SOLE_PROPRIETORSHIP",
            "COMPANY",
        ])

        agent_status = (
            "SUSPENDED" if i % 20 == 0
            else "ACTIVE"
        )

        onboarding_date = (
            now.date() - timedelta(
                days=rng.randint(90, 1200)
            )
        )

        agent_rows.append((
            agent_id,
            ref,
            f"Agent Business {i:03d}",
            agent_type,
            business_type,
            agent_status,
            onboarding_date,
            district,
            upazila,
        ))

        for outlet_no in range(1, outlets_per_agent + 1):
            outlet_ref = f"OUT{i:04d}{outlet_no:02d}"
            outlet_id = stable_uuid("outlet", outlet_ref)
            float_ref = f"FLT{i:04d}{outlet_no:02d}"
            float_id = stable_uuid("float", float_ref)

            outlet_status = (
                "SUSPENDED"
                if agent_status == "SUSPENDED"
                else "ACTIVE"
            )

            outlet_rows.append((
                outlet_id,
                outlet_ref,
                agent_id,
                f"{district} {upazila} Outlet {outlet_no}",
                district,
                upazila,
                outlet_status,
                now - timedelta(
                    days=rng.randint(30, 800)
                ),
            ))

            float_rows.append((
                float_id,
                float_ref,
                outlet_id,
                "BDT",
                0,
                0,
                0,
            ))

            outlet_ids.append(outlet_id)

    # --------------------------------------------------
    # 3. Merchants and terminals
    # --------------------------------------------------

    merchant_rows = []
    merchant_records = []
    terminal_rows = []

    for i in range(1, merchants_count + 1):
        ref = f"MRC{i:04d}"
        merchant_id = stable_uuid("merchant", ref)

        district = rng.choice(districts)
        merchant_type = (
            "CORPORATE" if i % 20 == 0
            else "ONLINE" if i % 7 == 0
            else "RETAIL_CHAIN" if i % 5 == 0
            else "SMALL_BUSINESS"
        )

        status = "PENDING" if i % 25 == 0 else "ACTIVE"

        merchant_rows.append((
            merchant_id,
            ref,
            f"{rng.choice(MERCHANT_CATEGORIES)} Merchant {i:03d}",
            rng.choice(MERCHANT_CATEGORIES),
            merchant_type,
            status,
            rng.choice(["DAILY", "WEEKLY"]),
            district,
            (now.date() - timedelta(
                days=rng.randint(30, 1000)
            )),
        ))

        merchant_records.append({
            "merchant_id": merchant_id,
            "merchant_status": status,
        })

    terminal_types = ["QR", "QR", "QR", "POS", "WEB", "API"]

    for i in range(1, terminals_count + 1):
        ref = f"TRM{i:05d}"
        terminal_id = stable_uuid("terminal", ref)

        merchant = merchant_records[
            (i - 1) % len(merchant_records)
        ]

        terminal_status = (
            "ACTIVE"
            if merchant["merchant_status"] == "ACTIVE"
            else "INACTIVE"
        )

        terminal_rows.append((
            terminal_id,
            ref,
            merchant["merchant_id"],
            terminal_types[(i - 1) % len(terminal_types)],
            terminal_status,
        ))

    # --------------------------------------------------
    # 4. Service providers
    # --------------------------------------------------

    provider_rows = []

    for i, (provider_type, provider_name) in enumerate(
        PROVIDERS, start=1
    ):
        ref = f"PROV{i:04d}"

        provider_rows.append((
            stable_uuid("provider", ref),
            ref,
            provider_name,
            provider_type,
            "ACTIVE",
        ))

    # --------------------------------------------------
    # 5. MongoDB device profiles
    # --------------------------------------------------

    selected_customers = rng.sample(
        customer_ids,
        devices_count
    )

    device_documents = []

    for i, customer_id in enumerate(
        selected_customers, start=1
    ):
        device_type = rng.choices(
            ["ANDROID", "IOS", "WEB"],
            weights=[80, 18, 2],
            k=1,
        )[0]

        platform = (
            "WEB" if device_type == "WEB"
            else "MOBILE"
        )

        if device_type == "ANDROID":
            os_version = f"Android {rng.randint(12, 16)}"
        elif device_type == "IOS":
            os_version = f"iOS {rng.randint(16, 19)}"
        else:
            os_version = "Browser"

        registered_at = now - timedelta(
            days=rng.randint(60, 700)
        )

        last_seen_at = now - timedelta(
            days=rng.randint(0, 30)
        )

        device_documents.append({
            "device_id": f"DEV{i:06d}",
            "customer_id": customer_id,
            "device_type": device_type,
            "platform": platform,
            "os_version": os_version,
            "app_version": rng.choice([
                "5.0.0",
                "5.1.0",
                "5.2.0",
                "5.3.0",
            ]),
            "trust_status": rng.choices(
                ["TRUSTED", "UNVERIFIED", "BLOCKED"],
                weights=[92, 7, 1],
                k=1,
            )[0],
            "registered_at": registered_at,
            "last_seen_at": last_seen_at,
        })

    return {
        "customers": customer_rows,
        "profiles": profile_rows,
        "wallets": wallet_rows,
        "balances": balance_rows,
        "agents": agent_rows,
        "outlets": outlet_rows,
        "floats": float_rows,
        "merchants": merchant_rows,
        "terminals": terminal_rows,
        "providers": provider_rows,
        "devices": device_documents,
    }
