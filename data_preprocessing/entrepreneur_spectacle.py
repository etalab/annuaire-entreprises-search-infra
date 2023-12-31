import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from dag_datalake_sirene.config import URL_ENTREPRENEUR_SPECTACLE


def preprocess_spectacle_data(data_dir):
    retries = Retry(total=10, backoff_factor=1, status_forcelist=[ 429, 500, 502, 503, 504 ])
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retries))

    r = session.get(URL_ENTREPRENEUR_SPECTACLE)
    with open(data_dir + "spectacle-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)

    df_spectacle = pd.read_csv(data_dir + "spectacle-download.csv", dtype=str, sep=";")
    df_spectacle["siren"] = df_spectacle[
        "siren_personne_physique_siret_personne_morale"
    ].str[:9]
    df_spectacle = df_spectacle[["siren", "statut_du_recepisse"]]
    df_spectacle["statut_du_recepisse"] = df_spectacle["statut_du_recepisse"].apply(
        lambda x: "valide" if x == "Valide" else "invalide"
    )

    df_spectacle = df_spectacle[df_spectacle["siren"].notna()]
    df_spectacle_clean = (
        df_spectacle.groupby("siren")["statut_du_recepisse"].unique().reset_index()
    )
    # If at least one of `statut` values is valid, then the value we keep is `valide
    df_spectacle_clean["statut_entrepreneur_spectacle"] = df_spectacle_clean[
        "statut_du_recepisse"
    ].apply(lambda list_statuts: "valide" if "valide" in list_statuts else "invalide")
    df_spectacle_clean["est_entrepreneur_spectacle"] = True
    df_spectacle_clean.drop("statut_du_recepisse", axis=1, inplace=True)
    return df_spectacle_clean
