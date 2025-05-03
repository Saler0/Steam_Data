setwd("C:/Proyectos/Licitaciones_Publicas")

# 1) Instala/carga librerías
install.packages(c("xml2", "dplyr", "purrr","tidyr"))
library(xml2); library(dplyr); library(purrr); library(tidyr)

# -------------------CATALOGOS---------------------------------------

# 1) URL del catálogo oficial de estados
url_catalogo_estados <- "https://contrataciondelestado.es/codice/cl/2.04/SyndicationContractFolderStatusCode-2.04.gc"
url_catalogo_party_type <- "http://contrataciondelestado.es/codice/cl/2.10/ContractingAuthorityCode-2.10.gc"
url_catalogo_activity <- "http://contrataciondelestado.es/codice/cl/2.10/ContractingAuthorityActivityCode-2.10.gc"
url_catalogo_type    <- "http://contrataciondelestado.es/codice/cl/2.08/ContractCode-2.08.gc"
url_catalogo_subtype <- "http://contrataciondelestado.es/codice/cl/1.04/GoodsContractCode-1.04.gc"
url_catalogo_cpv <- "https://contrataciondelestado.es/codice/cl/2.04/CPV2008-2.04.gc"
CountrySubentityCode <- "http://contrataciondelestado.es/codice/cl/2.08/NUTS-2021.gc"
IdentificationCode_country <- "http://contrataciondelestado.es/codice/cl/2.08/CountryIdentificationCode-2.08.gc"
FundingProgramCode <- "http://contrataciondelestado.es/codice/cl/2.08/FundingProgramCode-2.08.gc"
ProcurementNationalLegislationCode <- "https://contrataciondelestado.es/codice/cl/2.08/ProcurementNationalLegislationCode-2.08.gc"
EvaluationCriteriaTypeCode_technical <- "http://contrataciondelestado.es/codice/cl/2.0/TechnicalCapabilityTypeCode-2.0.gc"
EvaluationCriteriaTypeCode_financial <- "http://contrataciondelestado.es/codice/cl/2.0/FinancialCapabilityTypeCode-2.0.gc"
RequirementTypeCode <- "http://contrataciondelestado.es/codice/cl/2.08/DeclarationTypeCode-2.08.gc"
ProcedureCode <- "https://contrataciondelestado.es/codice/cl/2.07/SyndicationTenderingProcessCode-2.07.gc"
ContractingSystemCode <- "http://contrataciondelestado.es/codice/cl/2.08/ContractingSystemTypeCode-2.08.gc"
SubmissionMethodCode <- "http://contrataciondelestado.es/codice/cl/1.04/TenderDeliveryCode-1.04.gc"
NoticeTypeCode <- "http://contrataciondelestado.es/codice/cl/2.11/TenderingNoticeTypeCode-2.11.gc"
DocumentTypeCode <- "http://contrataciondelestado.es/codice/cl/2.08/GeneralContractDocuments-2.08.gc"


# 2) Función genérica para parsear catálogos de la forma Row/Value[@ColumnRef='code']/SimpleValue + 'nombre'
parse_catalog <- function(url, col_code_name, col_name_name) {
  doc  <- read_xml(url)
  rows <- xml_find_all(doc, ".//Row")
  map_df(rows, function(r) {
    code   <- xml_text(xml_find_first(r, ".//Value[@ColumnRef='code']/SimpleValue"))
    nombre <- xml_text(xml_find_first(r, ".//Value[@ColumnRef='nombre']/SimpleValue"))
    tibble(
      !!col_code_name := code,
      !!col_name_name := nombre
    )
  })
}

# 3) Genera todos los lookups con una sola llamada cada uno
status_lookup   <- parse_catalog(url_catalogo_estados,      "status_code",   "status_name")
party_lookup    <- parse_catalog(url_catalogo_party_type,   "party_type_code", "party_type_name")
activity_lookup <- parse_catalog(url_catalogo_activity,     "activity_code", "activity_name")
type_lookup     <- parse_catalog(url_catalogo_type,         "type_code",     "type_name")
subtype_lookup  <- parse_catalog(url_catalogo_subtype,      "subtype_code",  "subtype_name")
cpv_lookup <- parse_catalog(url_catalogo_cpv, "cpv", "cpv_name")

# 4) Compruebarlos alguno de ellos
print(status_lookup)
print(party_lookup)
print(activity_lookup)
print(type_lookup)
print(subtype_lookup)
print(cpv_lookup)

# -----------------------FUNCION DE PARSING ATOM-----------------------------------

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
    
    # Extraemos todos los nombres dentro de ParentLocatedParty
    parent_nodes <- xml_find_all(
      e,
      ".//cpe:LocatedContractingParty//cpe:ParentLocatedParty//cac:PartyName//cbc:Name",
      ns
    )
    parent_chain <- if (length(parent_nodes)==0) {
      NA_character_
    } else {
      paste(xml_text(parent_nodes), collapse = " > ")
    }
    
    # Captura _todos_ los códigos CPV
    cpv_nodes <- xml_find_all(
      e,
      ".//cac:RequiredCommodityClassification//cbc:ItemClassificationCode",
      ns
    )
    # Extrae los textos y los pega con coma
    cpv_codes <- xml_text(cpv_nodes)
    cpv_concat <- if (length(cpv_codes)==0) NA_character_ else paste(cpv_codes, collapse = ",")
    
    
    tibble(
      id             = text_or_na(xml_find_first(e, ".//atom:id",                                        ns)),
      resumen        = text_or_na(xml_find_first(e, ".//atom:summary",                                   ns)),
      titulo         = text_or_na(xml_find_first(e, ".//atom:title",                                     ns)),
      fecha          = text_or_na(xml_find_first(e, ".//atom:updated",                                   ns)),
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

      ## Cadena de padres (jerarquía)
      contracting_party_hierarchy       = parent_chain,
      
      # Extrae los códigos de tipo y subtipo
      type_code         = text_or_na(xml_find_first(e, ".//cac:ProcurementProject//cbc:TypeCode",    ns)),
      subtype_code      = text_or_na(xml_find_first(e, ".//cac:ProcurementProject//cbc:SubTypeCode", ns)),      
      
      estimated_overall_amount   = text_or_na(xml_find_first(e, ".//cac:BudgetAmount//cbc:EstimatedOverallContractAmount", ns)),
      TaxExclusiveAmount   = text_or_na(xml_find_first(e, ".//cac:BudgetAmount//cbc:TaxExclusiveAmount",       ns)),
      TotalAmount  = text_or_na(xml_find_first(e, ".//cac:BudgetAmount//cbc:TotalAmount",               ns)),
  
      cpv           = cpv_concat,
      adjudicatario  = text_or_na(xml_find_first(e, ".//cac:TenderResult//cac:WinningParty//cbc:Name",    ns)),
      contratante    = text_or_na(xml_find_first(e, ".//cpe:LocatedContractingParty//cac:PartyName//cbc:Name", ns))
      )
  })
}


# -----------------------MONTA EL DATAFRAME-----------------------------------

# 4) Aplica a todos los .atom de la carpeta
ruta_carpeta  <- "licitaciones_datos/licitacionesPerfilesContratanteCompleto3_202504/licitacionesPerfilesContratanteCompleto3_202504"
archivos_atom <- list.files(ruta_carpeta, pattern="\\.atom$", full.names=TRUE)


# 6) Hacer el parseo
licitaciones_df <- map_df(archivos_atom, parsear_atom)

# 7) Ahora, tras haber parseado tus .atom en `licitaciones_df` (que incluye columna `status_code`), basta hacer:

licitaciones_df <- licitaciones_df %>%
  # Une el lookup de estados
  left_join(status_lookup,  by = "status_code")  %>%
  relocate(status_name,   .after = status_code) %>%
  # Une el lookup de tipos de órgano
  left_join(party_lookup,   by = "party_type_code") %>%
  relocate(party_type_name, .after = party_type_code) %>%
  # Une el nuevo lookup de actividad
  left_join(activity_lookup, by = "activity_code") %>%
  relocate(activity_name,   .after = activity_code) %>%
  # Une el nuevo lookup de type/subtype           
  left_join(type_lookup,    by = "type_code")    %>% relocate(type_name,    .after = type_code)   %>%
  left_join(subtype_lookup, by = "subtype_code") %>% relocate(subtype_name, .after = subtype_code) %>%
  # Une el nuevo lookup de cpv 
  mutate(cpv_list = strsplit(cpv, ",")) %>%
  unnest(cpv_list) %>%                               # una fila por cada código
  left_join(cpv_lookup, by = c("cpv_list" = "cpv")) %>%  # trae cpv_name
  # agrupamos todo excepto los campos de cpv para volver a aplanar
  group_by(across(-c(cpv, cpv_list, cpv_name))) %>%
  summarize(
    cpv         = paste(unique(cpv_list), collapse = ","),    # vuelve a concatenar
    cpv_name    = paste(unique(cpv_name), collapse = ","),    # concatena nombres
    .groups     = "drop"
  ) %>%
  ungroup() %>%
  relocate(cpv_name, .after = cpv)  # opcional: para que cpv_name quede junto a cpv

# 5) Comprueba el resultado
print(head(licitaciones_df), width = Inf)



