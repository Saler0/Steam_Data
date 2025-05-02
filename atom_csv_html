import xml.etree.ElementTree as ET
import pandas as pd
import requests
from urllib.parse import urlparse
from lxml import etree
from io import StringIO
import json

# Diccionario para mapear encabezados largos a nombres más cortos y descriptivos
HEADER_MAPPING = {
    'Entry_ID': 'ID_Entrada',
    'Entry_Title': 'Titulo',
    'Entry_Updated': 'Fecha_Actualizacion',
    'Entry_Published': 'Fecha_Publicacion',
    'Link_': 'Enlace_Principal',
    'Link__Title': 'Titulo_Enlace_Principal',
    'ContractFolderStatus/ContractFolderID': 'ID_Licitacion',
    'ContractFolderStatus/ContractFolderStatusCode': 'Estado',
    'ContractFolderStatus/ContractFolderStatusCode@languageID': 'Idioma_Estado',
    'ContractFolderStatus/ContractFolderStatusCode@listURI': 'URI_Diccionario_Estado',
    'ContractFolderStatus/LocatedContractingParty/ContractingPartyTypeCode': 'Tipo_Entidad_Contratante',
    'ContractFolderStatus/LocatedContractingParty/Party/WebsiteURI': 'Sitio_Web_Entidad',
    'ContractFolderStatus/LocatedContractingParty/Party/PartyIdentification/ID': 'ID_Entidad_Contratante',
    'ContractFolderStatus/LocatedContractingParty/Party/PartyIdentification/ID@schemeName': 'Esquema_ID_Entidad',
    'ContractFolderStatus/LocatedContractingParty/Party/PartyName/Name': 'Organo_Contratacion',
    'ContractFolderStatus/LocatedContractingParty/Party/PostalAddress/CityName': 'Ciudad_Entidad',
    'ContractFolderStatus/LocatedContractingParty/Party/PostalAddress/PostalZone': 'Codigo_Postal_Entidad',
    'ContractFolderStatus/LocatedContractingParty/Party/PostalAddress/AddressLine/Line': 'Direccion_Entidad',
    'ContractFolderStatus/LocatedContractingParty/Party/PostalAddress/Country/IdentificationCode': 'Codigo_Pais_Entidad',
    'ContractFolderStatus/LocatedContractingParty/Party/PostalAddress/Country/Name': 'Pais_Entidad',
    'ContractFolderStatus/LocatedContractingParty/Party/Contact/Name': 'Contacto_Entidad',
    'ContractFolderStatus/LocatedContractingParty/Party/Contact/Telephone': 'Telefono_Entidad',
    'ContractFolderStatus/LocatedContractingParty/Party/Contact/Telefax': 'Fax_Entidad',
    'ContractFolderStatus/LocatedContractingParty/Party/Contact/ElectronicMail': 'Email_Entidad',
    'ContractFolderStatus/ProcurementProject/Name': 'Nombre_Proyecto',
    'ContractFolderStatus/ProcurementProject/TypeCode': 'Tipo_Contrato',
    'ContractFolderStatus/ProcurementProject/TypeCode@listURI': 'URI_Diccionario_Tipo_Contrato',
    'ContractFolderStatus/ProcurementProject/SubTypeCode': 'Subtipo_Contrato',
    'ContractFolderStatus/ProcurementProject/SubTypeCode@listURI': 'URI_Diccionario_Subtipo_Contrato',
    'ContractFolderStatus/ProcurementProject/BudgetAmount/EstimatedOverallContractAmount': 'Importe_Estimado',
    'ContractFolderStatus/ProcurementProject/BudgetAmount/EstimatedOverallContractAmount@currencyID': 'Moneda_Importe_Estimado',
    'ContractFolderStatus/ProcurementProject/BudgetAmount/TotalAmount': 'Importe_Total',
    'ContractFolderStatus/ProcurementProject/BudgetAmount/TotalAmount@currencyID': 'Moneda_Importe_Total',
    'ContractFolderStatus/ProcurementProject/BudgetAmount/TaxExclusiveAmount': 'Importe_Sin_Impuestos',
    'ContractFolderStatus/ProcurementProject/BudgetAmount/TaxExclusiveAmount@currencyID': 'Moneda_Importe_Sin_Impuestos',
    'ContractFolderStatus/ProcurementProject/RequiredCommodityClassification/ItemClassificationCode': 'Codigo_CPV',
    'ContractFolderStatus/ProcurementProject/RequiredCommodityClassification/ItemClassificationCode@listURI': 'URI_Diccionario_CPV',
    'ContractFolderStatus/ProcurementProject/RealizedLocation/CountrySubentity': 'Region',
    'ContractFolderStatus/ProcurementProject/RealizedLocation/CountrySubentityCode': 'Codigo_Region',
    'ContractFolderStatus/ProcurementProject/RealizedLocation/CountrySubentityCode@listURI': 'URI_Diccionario_Region',
    'ContractFolderStatus/ProcurementProject/RealizedLocation/Address/Country/IdentificationCode': 'Codigo_Pais_Proyecto',
    'ContractFolderStatus/ProcurementProject/RealizedLocation/Address/Country/Name': 'Pais_Proyecto',
    'ContractFolderStatus/TenderResult/ResultCode': 'Resultado_Adjudicacion',
    'ContractFolderStatus/TenderResult/ResultCode@listURI': 'URI_Diccionario_Resultado',
    'ContractFolderStatus/TenderResult/Description': 'Motivo_Adjudicacion',
    'ContractFolderStatus/TenderResult/Contract/IssueDate': 'Fecha_Adjudicacion',
    'ContractFolderStatus/TenderResult/WinningParty/PartyIdentification/ID': 'ID_Adjudicatario',
    'ContractFolderStatus/TenderResult/WinningParty/PartyIdentification/ID@schemeName': 'Esquema_ID_Adjudicatario',
    'ContractFolderStatus/TenderResult/WinningParty/PartyName/Name': 'Adjudicatario',
    'ContractFolderStatus/TenderResult/AwardedTenderedProject/LegalMonetaryTotal/TaxExclusiveAmount': 'Importe_Adjudicado_Sin_Impuestos',
    'ContractFolderStatus/TenderResult/AwardedTenderedProject/LegalMonetaryTotal/TaxExclusiveAmount@currencyID': 'Moneda_Importe_Adjudicado_Sin_Impuestos',
    'ContractFolderStatus/TenderResult/AwardedTenderedProject/LegalMonetaryTotal/PayableAmount': 'Importe_Adjudicado_Total',
    'ContractFolderStatus/TenderResult/AwardedTenderedProject/LegalMonetaryTotal/PayableAmount@currencyID': 'Moneda_Importe_Adjudicado_Total',
    'ContractFolderStatus/TenderingTerms/VariantConstraintIndicator': 'Restriccion_Variantes',
    'ContractFolderStatus/TenderingTerms/Language/ID': 'Idioma_Procedimiento',
    'ContractFolderStatus/TenderingProcess/ProcedureCode': 'Procedimiento',
    'ContractFolderStatus/TenderingProcess/ProcedureCode@listURI': 'URI_Diccionario_Procedimiento',
    'ContractFolderStatus/TenderingProcess/UrgencyCode': 'Urgencia',
    'ContractFolderStatus/TenderingProcess/UrgencyCode@listURI': 'URI_Diccionario_Urgencia',
    'ContractFolderStatus/TenderingProcess/TenderSubmissionDeadlinePeriod/EndDate': 'Fecha_Limite_Presentacion',
    'ContractFolderStatus/TenderingProcess/TenderSubmissionDeadlinePeriod/EndTime': 'Hora_Limite_Presentacion',
    'ContractFolderStatus/LegalDocumentReference/ID': 'ID_Documento_Legal',
    'ContractFolderStatus/LegalDocumentReference/Attachment/ExternalReference/URI': 'URI_Documento_Legal',
    'ContractFolderStatus/LegalDocumentReference/Attachment/ExternalReference/DocumentHash': 'Hash_Documento_Legal',
    'ContractFolderStatus/TechnicalDocumentReference/ID': 'ID_Documento_Tecnico',
    'ContractFolderStatus/TechnicalDocumentReference/Attachment/ExternalReference/URI': 'URI_Documento_Tecnico',
    'ContractFolderStatus/TechnicalDocumentReference/Attachment/ExternalReference/DocumentHash': 'Hash_Documento_Tecnico',
    'ContractFolderStatus/ValidNoticeInfo/NoticeTypeCode': 'Tipo_Aviso',
    'ContractFolderStatus/ValidNoticeInfo/NoticeTypeCode@listURI': 'URI_Diccionario_Tipo_Aviso',
    'ContractFolderStatus/ValidNoticeInfo/AdditionalPublicationStatus/PublicationMediaName': 'Medio_Publicacion',
    'ContractFolderStatus/ValidNoticeInfo/AdditionalPublicationStatus/AdditionalPublicationDocumentReference/IssueDate': 'Fecha_Publicacion_Adicional',
    'ContractFolderStatus/TenderResult/StartDate': 'Fecha_Inicio_Contrato',
    'ContractFolderStatus/TenderResult/Contract/ID': 'ID_Contrato',
    'ContractFolderStatus/TenderingTerms/RequiredFinancialGuarantee/GuaranteeTypeCode': 'Tipo_Garantia',
    'ContractFolderStatus/TenderingTerms/RequiredFinancialGuarantee/GuaranteeTypeCode@listURI': 'URI_Diccionario_Tipo_Garantia',
    'ContractFolderStatus/TenderingTerms/RequiredFinancialGuarantee/AmountRate': 'Tasa_Garantia',
    'ContractFolderStatus/ProcurementProject/PlannedPeriod/DurationMeasure': 'Duracion_Proyecto',
    'ContractFolderStatus/ProcurementProject/PlannedPeriod/DurationMeasure@unitCode': 'Unidad_Duracion_Proyecto',
    'ContractFolderStatus/TenderResult/AwardDate': 'Fecha_Adjudicacion_2',
    'ContractFolderStatus/ProcurementProject/RealizedLocation/Address/CityName': 'Ciudad_Proyecto',
    'ContractFolderStatus/ProcurementProject/RealizedLocation/Address/PostalZone': 'Codigo_Postal_Proyecto',
    'ContractFolderStatus/ProcurementProject/PlannedPeriod/StartDate': 'Fecha_Inicio_Proyecto',
    'ContractFolderStatus/ProcurementProject/ContractExtension/OptionsDescription': 'Opciones_Extension',
    'ContractFolderStatus/TenderResult/ReceivedTenderQuantity': 'Cantidad_Ofertas_Recibidas',
    'ContractFolderStatus/TenderingTerms/TendererQualificationRequest/TechnicalEvaluationCriteria/EvaluationCriteriaTypeCode': 'Criterio_Evaluacion_Tecnica',
    'ContractFolderStatus/TenderingTerms/TendererQualificationRequest/TechnicalEvaluationCriteria/EvaluationCriteriaTypeCode@listURI': 'URI_Diccionario_Criterio_Tecnico',
    'ContractFolderStatus/TenderingTerms/TendererQualificationRequest/TechnicalEvaluationCriteria/Description': 'Descripcion_Criterio_Tecnico',
    'ContractFolderStatus/TenderingTerms/TendererQualificationRequest/FinancialEvaluationCriteria/EvaluationCriteriaTypeCode': 'Criterio_Evaluacion_Financiera',
    'ContractFolderStatus/TenderingTerms/TendererQualificationRequest/FinancialEvaluationCriteria/EvaluationCriteriaTypeCode@listURI': 'URI_Diccionario_Criterio_Financiero',
    'ContractFolderStatus/TenderingTerms/TendererQualificationRequest/FinancialEvaluationCriteria/Description': 'Descripcion_Criterio_Financiero',
    'ContractFolderStatus/TenderingTerms/TendererQualificationRequest/SpecificTendererRequirement/RequirementTypeCode': 'Requisito_Especifico',
    'ContractFolderStatus/TenderingTerms/TendererQualificationRequest/SpecificTendererRequirement/RequirementTypeCode@listURI': 'URI_Diccionario_Requisito',
    'ContractFolderStatus/TenderingProcess/DocumentAvailabilityPeriod/EndDate': 'Fecha_Fin_Disponibilidad_Documentos',
    'ContractFolderStatus/TenderingProcess/DocumentAvailabilityPeriod/EndTime': 'Hora_Fin_Disponibilidad_Documentos',
    'ContractFolderStatus/TenderingTerms/TendererQualificationRequest/Description': 'Descripcion_Requisitos',
    'ContractFolderStatus/AdditionalDocumentReference/ID': 'ID_Documento_Adicional',
    'ContractFolderStatus/AdditionalDocumentReference/Attachment/ExternalReference/URI': 'URI_Documento_Adicional',
    'ContractFolderStatus/AdditionalDocumentReference/Attachment/ExternalReference/DocumentHash': 'Hash_Documento_Adicional',
    'ContractFolderStatus/TenderingTerms/FundingProgramCode': 'Codigo_Programa_Financiacion',
    'ContractFolderStatus/TenderingTerms/FundingProgramCode@listURI': 'URI_Diccionario_Programa_Financiacion',
    'ContractFolderStatus/TenderingTerms/TendererQualificationRequest/RequiredBusinessClassificationScheme/ID': 'ID_Clasificacion_Empresa',
    'ContractFolderStatus/TenderingTerms/TendererQualificationRequest/RequiredBusinessClassificationScheme/ClassificationCategory/CodeValue': 'Valor_Clasificacion_Empresa',
    'ContractFolderStatus/TenderingTerms/AllowedSubcontractTerms/Rate': 'Tasa_Subcontratacion',
    'ContractFolderStatus/TenderingProcess/TenderSubmissionDeadlinePeriod/Description': 'Descripcion_Plazo_Presentacion',
    'ContractFolderStatus/ProcurementProject/ContractExtension/OptionValidityPeriod/Description': 'Descripcion_Validez_Opciones',
    'ContractFolderStatus/ProcurementProjectLot/ID': 'ID_Lote',
    'ContractFolderStatus/ProcurementProjectLot/ID@schemeName': 'Esquema_ID_Lote',
    'ContractFolderStatus/ProcurementProjectLot/ProcurementProject/Name': 'Nombre_Lote',
    'ContractFolderStatus/ProcurementProjectLot/ProcurementProject/BudgetAmount/TotalAmount': 'Importe_Total_Lote',
    'ContractFolderStatus/ProcurementProjectLot/ProcurementProject/BudgetAmount/TotalAmount@currencyID': 'Moneda_Importe_Total_Lote',
    'ContractFolderStatus/ProcurementProjectLot/ProcurementProject/BudgetAmount/TaxExclusiveAmount': 'Importe_Sin_Impuestos_Lote',
    'ContractFolderStatus/ProcurementProjectLot/ProcurementProject/BudgetAmount/TaxExclusiveAmount@currencyID': 'Moneda_Importe_Sin_Impuestos_Lote',
    'ContractFolderStatus/ProcurementProjectLot/ProcurementProject/RequiredCommodityClassification/ItemClassificationCode': 'Codigo_CPV_Lote',
    'ContractFolderStatus/ProcurementProjectLot/ProcurementProject/RequiredCommodityClassification/ItemClassificationCode@listURI': 'URI_Diccionario_CPV_Lote',
    'ContractFolderStatus/TenderResult/AwardedTenderedProject/ProcurementProjectLotID': 'ID_Lote_Adjudicado',
    'ContractFolderStatus/TenderingTerms/RequiredFinancialGuarantee/LiabilityAmount': 'Monto_Garantia',
    'ContractFolderStatus/TenderingTerms/RequiredFinancialGuarantee/LiabilityAmount@currencyID': 'Moneda_Monto_Garantia',
    'ContractFolderStatus/LocatedContractingParty/ParentLocatedParty/PartyName/Name': 'Entidad_Superior_1',
    'ContractFolderStatus/LocatedContractingParty/ParentLocatedParty/ParentLocatedParty/PartyName/Name': 'Entidad_Superior_2',
    'ContractFolderStatus/LocatedContractingParty/ParentLocatedParty/ParentLocatedParty/ParentLocatedParty/PartyName/Name': 'Entidad_Superior_3',
    'ContractFolderStatus/LocatedContractingParty/ParentLocatedParty/ParentLocatedParty/ParentLocatedParty/ParentLocatedParty/PartyName/Name': 'Entidad_Superior_4',
    'ContractFolderStatus/LocatedContractingParty/ParentLocatedParty/ParentLocatedParty/ParentLocatedParty/ParentLocatedParty/ParentLocatedParty/PartyName/Name': 'Entidad_Superior_5',
    'ContractFolderStatus/TenderingTerms/PriceRevisionFormulaDescription': 'Formula_Revision_Precio',
    'ContractFolderStatus/TenderResult/LowerTenderAmount': 'Oferta_Mas_Baja',
    'ContractFolderStatus/TenderResult/LowerTenderAmount@currencyID': 'Moneda_Oferta_Mas_Baja',
    'ContractFolderStatus/TenderResult/HigherTenderAmount': 'Oferta_Mas_Alta',
    'ContractFolderStatus/TenderResult/HigherTenderAmount@currencyID': 'Moneda_Oferta_Mas_Alta',
    'ContractFolderStatus/TenderingTerms/FundingProgram': 'Programa_Financiacion',
    'ContractFolderStatus/TenderingProcess/ContractingSystemCode': 'Sistema_Contratacion',
    'ContractFolderStatus/TenderingProcess/ContractingSystemCode@listURI': 'URI_Diccionario_Sistema_Contratacion',
    'ContractFolderStatus/ProcurementProject/PlannedPeriod/EndDate': 'Fecha_Fin_Proyecto',
    'ContractFolderStatus/TenderingTerms/TendererQualificationRequest/PersonalSituation': 'Situacion_Personal',
    'ContractFolderStatus/LocatedContractingParty/ParentLocatedParty/ParentLocatedParty/ParentLocatedParty/ParentLocatedParty/ParentLocatedParty/ParentLocatedParty/PartyName/Name': 'Entidad_Superior_6',
    'ContractFolderStatus/TenderingProcess/EconomicOperatorShortList/ExpectedQuantity': 'Cantidad_Esperada_Operadores',
    'ContractFolderStatus/TenderingProcess/EconomicOperatorShortList/MinimumQuantity': 'Cantidad_Minima_Operadores',
    'ContractFolderStatus/TenderingProcess/EconomicOperatorShortList/LimitationDescription': 'Descripcion_Limitacion_Operadores',
    'ContractFolderStatus/ContractModification/ID': 'ID_Modificacion_Contrato',
    'ContractFolderStatus/ContractModification/Note': 'Nota_Modificacion_Contrato',
    'ContractFolderStatus/ContractModification/ContractModificationDurationMeasure': 'Duracion_Modificacion_Contrato',
    'ContractFolderStatus/ContractModification/ContractModificationDurationMeasure@unitCode': 'Unidad_Duracion_Modificacion',
    'ContractFolderStatus/ContractModification/FinalDurationMeasure': 'Duracion_Final_Contrato',
    'ContractFolderStatus/ContractModification/FinalDurationMeasure@unitCode': 'Unidad_Duracion_Final',
    'ContractFolderStatus/ContractModification/ContractID': 'ID_Contrato_Modificado',
    'ContractFolderStatus/ContractModification/ContractModificationLegalMonetaryTotal/TaxExclusiveAmount': 'Importe_Modificacion_Sin_Impuestos',
    'ContractFolderStatus/ContractModification/ContractModificationLegalMonetaryTotal/TaxExclusiveAmount@currencyID': 'Moneda_Importe_Modificacion_Sin_Impuestos',
    'ContractFolderStatus/ContractModification/FinalLegalMonetaryTotal/TaxExclusiveAmount': 'Importe_Final_Sin_Impuestos',
    'ContractFolderStatus/ContractModification/FinalLegalMonetaryTotal/TaxExclusiveAmount@currencyID': 'Moneda_Importe_Final_Sin_Impuestos',
    'ContractFolderStatus/TenderingTerms/AllowedSubcontractTerms/Description': 'Descripcion_Subcontratacion',
    'ContractFolderStatus/TenderingTerms/RequiredCurriculaIndicator': 'Indicador_Curricula_Requerida',
}

# Función para extraer texto de un elemento XML
def get_text(element, xpath, namespaces=None, default=''):
    if element is None:
        return default
    found = element.find(xpath, namespaces=namespaces)
    return found.text if found is not None else default

# Función para descargar y parsear diccionarios desde enlaces
def fetch_dictionary(url):
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        content_type = response.headers.get('content-type', '')

        if 'xml' in content_type:
            parser = etree.XMLParser()
            tree = etree.parse(StringIO(response.text), parser)
            root = tree.getroot()
            dictionary = {}
            for item in root.findall('.//item'):  # Ajustar según estructura real
                key = get_text(item, 'key')
                value = get_text(item, 'value')
                dictionary[key] = value
            return dictionary
        elif 'json' in content_type:
            return response.json()
        else:
            return {'raw': response.text}
    except requests.RequestException as e:
        print(f"Error al descargar {url}: {e}")
        return {}

# Función para extraer todos los datos de un elemento XML recursivamente
def extract_all_data(element, namespaces, prefix=''):
    data = {}
    if element is None:
        return data

    # Extraer texto si el elemento es una hoja
    if element.text and element.text.strip():
        data[prefix] = element.text.strip()

    # Procesar atributos
    for attr, value in element.attrib.items():
        data[f"{prefix}@{attr}"] = value

    # Procesar hijos
    for child in element:
        child_tag = child.tag.split('}')[-1]  # Remover namespace
        child_prefix = f"{prefix}/{child_tag}" if prefix else child_tag
        child_data = extract_all_data(child, namespaces, child_prefix)
        data.update(child_data)

    return data

# Función para procesar el archivo XML
def parse_licitaciones(xml_file):
    # Parsear el archivo XML
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Definir los namespaces
    namespaces = {
        'atom': 'http://www.w3.org/2005/Atom',
        'cbc-place-ext': 'urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2',
        'cac-place-ext': 'urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2',
        'cbc': 'urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2',
        'cac': 'urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2',
    }

    # Lista para almacenar los datos de cada licitación
    licitaciones = []
    dictionaries = {}

    # Iterar sobre cada entrada (<entry>)
    for entry in root.findall('atom:entry', namespaces):
        licitacion = {}

        # Extraer datos básicos de la entrada
        licitacion['Entry_ID'] = get_text(entry, 'atom:id', namespaces)
        licitacion['Entry_Title'] = get_text(entry, 'atom:title', namespaces)
        licitacion['Entry_Updated'] = get_text(entry, 'atom:updated', namespaces)
        licitacion['Entry_Published'] = get_text(entry, 'atom:published', namespaces)

        # Extraer todos los enlaces
        links = entry.findall('atom:link', namespaces)
        for link in links:
            rel = link.get('rel', '')
            href = link.get('href', '')
            title = link.get('title', '')
            licitacion[f"Link_{rel}"] = href
            licitacion[f"Link_{rel}_Title"] = title

            # Descargar diccionarios si el enlace es relevante
            if 'soap-envelope/encoding' in rel or 'alternate' in rel:
                parsed_url = urlparse(href)
                dict_key = parsed_url.path.split('/')[-1] or href
                if dict_key not in dictionaries:
                    dictionaries[dict_key] = fetch_dictionary(href)
                licitacion[f"Dictionary_{dict_key}"] = str(dictionaries[dict_key])

        # Extraer datos de ContractFolderStatus
        contract_status = entry.find('cac-place-ext:ContractFolderStatus', namespaces)
        if contract_status is not None:
            status_data = extract_all_data(contract_status, namespaces, 'ContractFolderStatus')
            licitacion.update(status_data)

        licitaciones.append(licitacion)

    # Crear un DataFrame
    df = pd.DataFrame(licitaciones)

    # Renombrar columnas usando el mapeo
    df = df.rename(columns=HEADER_MAPPING)

    # Añadir descripciones de diccionarios
    for column in df.columns:
        if 'URI_Diccionario' in column:
            # Extraer el nombre base del diccionario (por ejemplo, 'Estado' de 'URI_Diccionario_Estado')
            dict_key = column.replace('URI_Diccionario_', '')
            # Mapear el nombre del diccionario al nombre de la columna de código
            code_column_mapping = {
                'Estado': 'Estado',
                'Tipo_Contrato': 'Tipo_Contrato',
                'Subtipo_Contrato': 'Subtipo_Contrato',
                'CPV': 'Codigo_CPV',
                'Region': 'Codigo_Region',
                'Procedimiento': 'Procedimiento',
                'Urgencia': 'Urgencia',
                'Tipo_Aviso': 'Tipo_Aviso',
                'Tipo_Garantia': 'Tipo_Garantia',
                'Programa_Financiacion': 'Codigo_Programa_Financiacion',
                'Criterio_Tecnico': 'Criterio_Evaluacion_Tecnica',
                'Criterio_Financiero': 'Criterio_Evaluacion_Financiera',
                'Requisito': 'Requisito_Especifico',
                'Sistema_Contratacion': 'Sistema_Contratacion',
            }
            code_column = code_column_mapping.get(dict_key)
            desc_column = f"{dict_key}_Descripcion"
            if code_column and code_column in df.columns:
                dict_data = dictionaries.get(dict_key.lower(), {})
                df[desc_column] = df[code_column].map(dict_data).fillna(df[code_column])
            else:
                print(f"Columna de código '{code_column}' no encontrada para el diccionario '{dict_key}'")

    return df, dictionaries

# Ruta al archivo XML cambiar por la ruta correspondiente, solo permite un archivo atom
xml_file = r'D:\licitaciones_datos\licitacionesPerfilesContratanteCompleto3_2012\licitacionesPerfilesContratanteCompleto3_2012\licitacionesPerfilesContratanteCompleto3.atom'

# Procesar el archivo
df_licitaciones, dictionaries = parse_licitaciones(xml_file)

# Imprimir columnas para depuración
print("Columnas en el DataFrame:", df_licitaciones.columns.tolist())

# Mostrar las primeras filas del DataFrame
print(df_licitaciones.head())

# Exportar a CSV
df_licitaciones.to_csv('licitaciones_improved_output.csv', index=False, encoding='utf-8')

# Exportar a HTML
df_licitaciones.to_html('licitaciones_improved_output.html', index=False, escape=False)

# Guardar diccionarios en un archivo JSON
with open('dictionaries.json', 'w', encoding='utf-8') as f:
    json.dump(dictionaries, f, ensure_ascii=False, indent=2)
