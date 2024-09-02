# src/nhanes/nodes/demographic.py

import requests

def download_file(url: str, output_path: str) -> None:
    """
    Descarga un archivo desde una URL y lo guarda en la ruta especificada.

    Args:
        url (str): URL del archivo a descargar.
        output_path (str): Ruta donde se guardará el archivo descargado.
    """
    response = requests.get(url)
    response.raise_for_status()  # Levanta un error para códigos de estado HTTP no exitosos
    with open(output_path, 'wb') as file:
        file.write(response.content)
