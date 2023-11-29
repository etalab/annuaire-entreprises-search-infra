create_table_etablissements_query = """CREATE TABLE IF NOT EXISTS siret
            (
            siren,
            siret TEXT PRIMARY KEY,
            date_creation,
            tranche_effectif_salarie,
            caractere_employeur,
            annee_tranche_effectif_salarie,
            date_mise_a_jour,
            activite_principale_registre_metier,
            est_siege,
            numero_voie,
            type_voie,
            libelle_voie,
            code_postal,
            libelle_cedex,
            libelle_commune,
            commune,
            complement_adresse,
            complement_adresse_2,
            numero_voie_2,
            indice_repetition_2,
            type_voie_2,
            libelle_voie_2,
            commune_2,
            libelle_commune_2,
            cedex_2,
            libelle_cedex_2,
            cedex,
            date_debut_activite,
            distribution_speciale,
            distribution_speciale_2,
            etat_administratif_etablissement,
            enseigne_1,
            enseigne_2,
            enseigne_3,
            activite_principale,
            indice_repetition,
            nom_commercial,
            libelle_commune_etranger,
            code_pays_etranger,
            libelle_pays_etranger,
            libelle_commune_etranger_2,
            code_pays_etranger_2,
            libelle_pays_etranger_2,
            longitude,
            latitude,
            geo_adresse,
            geo_id)
            """
