from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


URL_BASE = "https://books.toscrape.com/"
ARCHIVO_EXCEL = Path("books.xlsx")
ARCHIVO_EXCEL_ALTERNATIVO = Path("books_actualizado.xlsx")
ARCHIVO_CSV = Path("books.csv")
ARCHIVO_JSON = Path("books.json")
TIEMPO_ESPERA_SEGUNDOS = 20
TOTAL_REINTENTOS = 3
ENCABEZADOS_HTTP = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
MAPA_CALIFICACION = {
    "One": 1,
    "Two": 2,
    "Three": 3,
    "Four": 4,
    "Five": 5,
}
TRADUCCION_CALIFICACION = {
    "One": "Uno",
    "Two": "Dos",
    "Three": "Tres",
    "Four": "Cuatro",
    "Five": "Cinco",
    "Unknown": "Desconocida",
}
TRADUCCION_DISPONIBILIDAD = {
    "In stock": "En stock",
}


@dataclass(slots=True)
class Libro:
    titulo: str
    precio_gbp: float
    disponibilidad: str
    calificacion_texto: str
    calificacion_valor: int
    url_producto: str
    url_imagen: str


def crear_sesion() -> Session:
    estrategia_reintentos = Retry(
        total=TOTAL_REINTENTOS,
        connect=TOTAL_REINTENTOS,
        read=TOTAL_REINTENTOS,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )

    sesion = requests.Session()
    sesion.headers.update(ENCABEZADOS_HTTP)
    adaptador = HTTPAdapter(max_retries=estrategia_reintentos)
    sesion.mount("http://", adaptador)
    sesion.mount("https://", adaptador)
    return sesion


def descargar_pagina(sesion: Session, url: str) -> str:
    respuesta = sesion.get(url, timeout=TIEMPO_ESPERA_SEGUNDOS)
    respuesta.raise_for_status()
    respuesta.encoding = respuesta.encoding or "utf-8"
    return respuesta.text


def convertir_precio(precio_texto: str) -> float:
    precio_limpio = (
        precio_texto.strip()
        .replace("Â", "")
        .replace("£", "")
        .replace(",", "")
    )
    return float(precio_limpio)


def extraer_calificacion(articulo: Tag) -> tuple[str, int]:
    elemento_calificacion = articulo.select_one("p.star-rating")
    if elemento_calificacion is None:
        return "Desconocida", 0

    clases = elemento_calificacion.get("class", [])
    calificacion_original = next(
        (valor for valor in clases if valor != "star-rating"),
        "Unknown",
    )
    calificacion_texto = TRADUCCION_CALIFICACION.get(
        calificacion_original,
        "Desconocida",
    )
    return calificacion_texto, MAPA_CALIFICACION.get(calificacion_original, 0)


def analizar_tarjeta_libro(articulo: Tag, url_pagina: str) -> Libro | None:
    enlace_titulo = articulo.select_one("h3 a")
    elemento_precio = articulo.select_one("p.price_color")
    elemento_disponibilidad = articulo.select_one("p.instock.availability")
    elemento_imagen = articulo.select_one("img")

    if enlace_titulo is None or elemento_precio is None:
        return None

    titulo = enlace_titulo.get("title", "").strip() or enlace_titulo.get_text(strip=True)
    enlace_relativo = enlace_titulo.get("href", "").strip()
    if not titulo or not enlace_relativo:
        return None

    disponibilidad = (
        elemento_disponibilidad.get_text(" ", strip=True)
        if elemento_disponibilidad is not None
        else "Desconocida"
    )
    disponibilidad = TRADUCCION_DISPONIBILIDAD.get(disponibilidad, disponibilidad)
    calificacion_texto, calificacion_valor = extraer_calificacion(articulo)
    url_imagen = (
        urljoin(url_pagina, elemento_imagen.get("src", "").strip())
        if elemento_imagen is not None
        else ""
    )

    return Libro(
        titulo=titulo,
        precio_gbp=convertir_precio(elemento_precio.get_text()),
        disponibilidad=disponibilidad,
        calificacion_texto=calificacion_texto,
        calificacion_valor=calificacion_valor,
        url_producto=urljoin(url_pagina, enlace_relativo),
        url_imagen=url_imagen,
    )


def analizar_libros(html: str, url_pagina: str) -> tuple[list[Libro], str | None]:
    sopa = BeautifulSoup(html, "html.parser")
    libros: list[Libro] = []

    for articulo in sopa.select("article.product_pod"):
        libro = analizar_tarjeta_libro(articulo, url_pagina)
        if libro is not None:
            libros.append(libro)

    enlace_siguiente = sopa.select_one("li.next a")
    url_siguiente = (
        urljoin(url_pagina, enlace_siguiente.get("href"))
        if enlace_siguiente is not None
        else None
    )
    return libros, url_siguiente


def scrapear_todos_los_libros(
    sesion: Session,
    url_inicial: str = URL_BASE,
) -> list[Libro]:
    libros: list[Libro] = []
    url_actual: str | None = url_inicial
    urls_visitadas: set[str] = set()

    while url_actual:
        if url_actual in urls_visitadas:
            raise RuntimeError("Se detecto un ciclo inesperado en la paginacion.")

        urls_visitadas.add(url_actual)
        html = descargar_pagina(sesion, url_actual)
        libros_pagina, url_actual = analizar_libros(html, url_actual)
        libros.extend(libros_pagina)

    return libros


def libros_a_dataframe(libros: Iterable[Libro]) -> pd.DataFrame:
    dataframe = pd.DataFrame(asdict(libro) for libro in libros)
    columnas_ordenadas = [
        "titulo",
        "precio_gbp",
        "disponibilidad",
        "calificacion_texto",
        "calificacion_valor",
        "url_producto",
        "url_imagen",
    ]
    dataframe = dataframe[columnas_ordenadas]

    return dataframe.rename(
        columns={
            "titulo": "titulo",
            "precio_gbp": "precio_gbp",
            "disponibilidad": "disponibilidad",
            "calificacion_texto": "calificacion_texto",
            "calificacion_valor": "calificacion_valor",
            "url_producto": "url_producto",
            "url_imagen": "url_imagen",
        }
    )


def guardar_excel(dataframe: pd.DataFrame) -> Path:
    try:
        dataframe.to_excel(ARCHIVO_EXCEL, index=False)
        return ARCHIVO_EXCEL
    except PermissionError:
        dataframe.to_excel(ARCHIVO_EXCEL_ALTERNATIVO, index=False)
        return ARCHIVO_EXCEL_ALTERNATIVO


def guardar_archivos(dataframe: pd.DataFrame) -> Path:
    dataframe.to_csv(ARCHIVO_CSV, index=False, encoding="utf-8-sig")
    dataframe.to_json(ARCHIVO_JSON, orient="records", indent=2, force_ascii=False)
    return guardar_excel(dataframe)


def mostrar_rutas_salida(ruta_excel: Path) -> None:
    print(f"Excel: {ruta_excel.resolve()}")
    print(f"CSV: {ARCHIVO_CSV.resolve()}")
    print(f"JSON: {ARCHIVO_JSON.resolve()}")


def principal() -> None:
    try:
        sesion = crear_sesion()
        libros = scrapear_todos_los_libros(sesion)

        if not libros:
            raise RuntimeError("No se encontraron libros en la pagina.")

        dataframe = libros_a_dataframe(libros)
        ruta_excel = guardar_archivos(dataframe)
    except requests.Timeout as error:
        print(
            "La solicitud tardo demasiado en responder incluso despues de reintentar. "
            "Intenta nuevamente en unos segundos."
        )
        raise SystemExit(1) from error
    except requests.RequestException as error:
        print(f"No se pudo descargar la pagina: {error}")
        raise SystemExit(1) from error
    except ValueError as error:
        print(f"No se pudo procesar uno de los precios: {error}")
        raise SystemExit(1) from error
    except RuntimeError as error:
        print(str(error))
        raise SystemExit(1) from error

    print(f"Libros encontrados: {len(libros)}")
    mostrar_rutas_salida(ruta_excel)


if __name__ == "__main__":
    principal()
