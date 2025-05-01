setwd("C:/Proyectos/Licitaciones_Publicas")

# 1) Instala/carga librerías
install.packages(c("xml2", "dplyr", "purrr"))
library(xml2); library(dplyr); library(purrr)

# -------------------CATALOGOS---------------------------------------

# 1) URL del catálogo oficial de estados
url_catalogo_estados <- "https://contrataciondelestado.es/codice/cl/2.04/SyndicationContractFolderStatusCode-2.04.gc"
url_catalogo_party_type <- "http://contrataciondelestado.es/codice/cl/2.10/ContractingAuthorityCode-2.10.gc"
url_catalogo_activity <- "http://contrataciondelestado.es/codice/cl/2.10/ContractingAuthorityActivityCode-2.10.gc"

# 2) Leer el XML del catálogo
cat_doc <- read_xml(url_catalogo_estados)
party_doc <- read_xml(url_catalogo_party_type)
activity_doc <- read_xml(url_catalogo_activity)

# 3) Definir namespaces (si los tiene) — en este caso el catálogo usa UBL, pero las etiquetas <Row> vienen sin prefijo
#    Así que podemos simplemente buscar las filas directamente
status_rows <- xml_find_all(cat_doc, ".//Row")
party_rows <- xml_find_all(party_doc, ".//Row")
activity_rows <- xml_find_all(activity_doc, ".//Row")

# 4) Extraer code y nombre de cada <Row>
status_lookup <- map_df(status_rows, function(r) {
  code   <- xml_text(xml_find_first(r, ".//Value[@ColumnRef='code']/SimpleValue"))
  nombre <- xml_text(xml_find_first(r, ".//Value[@ColumnRef='nombre']/SimpleValue"))
  tibble(status_code = code, status_name = nombre)
})

party_lookup <- map_df(party_rows, function(r) {
  code   <- xml_text(xml_find_first(r, ".//Value[@ColumnRef='code']/SimpleValue"))
  nombre <- xml_text(xml_find_first(r, ".//Value[@ColumnRef='nombre']/SimpleValue"))
  tibble(party_type_code = code, party_type_name = nombre)
})

activity_lookup <- map_df(activity_rows, function(r) {
  code   <- xml_text(xml_find_first(r, ".//Value[@ColumnRef='code']/SimpleValue"))
  nombre <- xml_text(xml_find_first(r, ".//Value[@ColumnRef='nombre']/SimpleValue"))
  tibble(activity_code = code, activity_name = nombre)
})

# 5) Vistazo al lookup
print(status_lookup)
print(party_lookup)
print(activity_lookup)

# -----------------------ARCHIVOS ATOM-----------------------------------

# 1) Define manualmente los namespaces que usa tu XML
ns <- c(
  atom = "http://www.w3.org/2005/Atom",
  cbc  = "urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2",
  cac  = "urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2",
  cbcpe = "urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2",
  cpe  = "urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2",
  at   = "http://purl.org/atompub/tombstones/1.0"
)
# EJEMPLO:
# cbcpe → para etiquetas <cbc-place-ext:…>
# cpe → para etiquetas <cac-place-ext:…>

# 2) Función segura para extraer texto o devolver NA
text_or_na <- function(node) {
  if (length(node) == 0) NA_character_ else xml_text(node)
}

# 3) Función de parsing, usando los prefijos de `ns`
parsear_atom <- function(path) {
  doc     <- read_xml(path)
  entries <- xml_find_all(doc, ".//atom:entry", ns)
  
  map_df(entries, function(e) {
    tibble(
      id             = text_or_na(xml_find_first(e, ".//atom:id",                                        ns)),
      resumen        = text_or_na(xml_find_first(e, ".//atom:summary",                                   ns)),
      titulo         = text_or_na(xml_find_first(e, ".//atom:title",                                     ns)),
      status_code    = text_or_na(xml_find_first(e, ".//cpe:ContractFolderStatus//cbcpe:ContractFolderStatusCode", ns)),
      party_type_code= text_or_na(xml_find_first(e, ".//cpe:LocatedContractingParty//cbc:ContractingPartyTypeCode", ns)),
      activity_code      = text_or_na(xml_find_first(e, ".//cpe:LocatedContractingParty//cbc:ActivityCode",               ns)),  
      
      # IDs múltiples según schemeName (contratante)
      dir3_id                  = text_or_na(xml_find_first(e, ".//cpe:LocatedContractingParty//cac:Party//cac:PartyIdentification[cbc:ID/@schemeName='DIR3']/cbc:ID", ns)),
      nif_id                   = text_or_na(xml_find_first(e, ".//cpe:LocatedContractingParty//cac:Party//cac:PartyIdentification[cbc:ID/@schemeName='NIF']/cbc:ID", ns)),
      platform_id              = text_or_na(xml_find_first(e, ".//cpe:LocatedContractingParty//cac:Party//cac:PartyIdentification[cbc:ID/@schemeName='ID_PLATAFORMA']/cbc:ID", ns)),
      
      # Nombre de la organización contratante
      party_name               = text_or_na(xml_find_first(e, ".//cpe:LocatedContractingParty//cac:Party//cac:PartyName/cbc:Name", ns)),
      
      # Dirección postal contratante
      party_city               = text_or_na(xml_find_first(e, ".//cpe:LocatedContractingParty//cac:Party//cac:PostalAddress/cbc:CityName", ns)),
      party_postal_zone        = text_or_na(xml_find_first(e, ".//cpe:LocatedContractingParty//cac:Party//cac:PostalAddress/cbc:PostalZone", ns)),
      party_address_line       = text_or_na(xml_find_first(e, ".//cpe:LocatedContractingParty//cac:Party//cac:PostalAddress//cac:AddressLine/cbc:Line", ns)),
      party_country_code       = text_or_na(xml_find_first(e, ".//cpe:LocatedContractingParty//cac:Party//cac:PostalAddress//cac:Country/cbc:IdentificationCode", ns)),
      party_country_name       = text_or_na(xml_find_first(e, ".//cpe:LocatedContractingParty//cac:Party//cac:PostalAddress//cac:Country/cbc:Name", ns)),
      
      # Contacto contratante
      party_contact_name       = text_or_na(xml_find_first(e, ".//cpe:LocatedContractingParty//cac:Party//cac:Contact/cbc:Name", ns)),
      party_contact_telephone  = text_or_na(xml_find_first(e, ".//cpe:LocatedContractingParty//cac:Party//cac:Contact/cbc:Telephone", ns)),
      party_contact_email      = text_or_na(xml_find_first(e, ".//cpe:LocatedContractingParty//cac:Party//cac:Contact/cbc:ElectronicMail", ns)),
      
      fecha          = text_or_na(xml_find_first(e, ".//atom:updated",                                   ns)),
      importe_base   = text_or_na(xml_find_first(e, ".//cac:BudgetAmount//cbc:TaxExclusiveAmount",       ns)),
      importe_total  = text_or_na(xml_find_first(e, ".//cac:BudgetAmount//cbc:TotalAmount",               ns)),
      cpv            = text_or_na(xml_find_first(e, ".//cac:RequiredCommodityClassification//cbc:ItemClassificationCode", ns)),
      adjudicatario  = text_or_na(xml_find_first(e, ".//cac:TenderResult//cac:WinningParty//cbc:Name",    ns)),
      contratante    = text_or_na(xml_find_first(e, ".//cpe:LocatedContractingParty//cac:PartyName//cbc:Name", ns))
      )
  })
}

# 4) Aplica a todos los .atom de la carpeta
ruta_carpeta  <- "licitaciones_datos/licitacionesPerfilesContratanteCompleto3_202504/licitacionesPerfilesContratanteCompleto3_202504"
archivos_atom <- list.files(ruta_carpeta, pattern="\\.atom$", full.names=TRUE)

# 6) Hacer el parseo
licitaciones_df <- map_df(archivos_atom, parsear_atom)

# 7) Ahora, tras haber parseado tus .atom en `licitaciones_df` (que incluye columna `status_code`), basta hacer:
library(dplyr)

licitaciones_df <- licitaciones_df %>%
  # Une el lookup de estados
  left_join(status_lookup,  by = "status_code")  %>%
  relocate(status_name,   .after = status_code) %>%
  # Une el lookup de tipos de órgano
  left_join(party_lookup,   by = "party_type_code") %>%
  relocate(party_type_name, .after = party_type_code) %>%
  # Une el nuevo lookup de actividad
  left_join(activity_lookup, by = "activity_code") %>%
  relocate(activity_name,   .after = activity_code)
           

# 5) Comprueba el resultado
print(head(licitaciones_df), width = Inf)



