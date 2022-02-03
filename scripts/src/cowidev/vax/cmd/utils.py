def normalize_country_name(country_name: str):
    return country_name.strip().replace("-", "_").replace(" ", "_").lower()
