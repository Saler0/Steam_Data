from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.services.pipelineService import (
    PipelineService,
    PipelineServiceType,
)
from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
from metadata.generated.schema.type.entityReference import EntityReference

from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
    AuthProvider,
)

def register_pipeline_metadata():
    # 1) Configuración de autenticación JWT para el ingestion-bot
    security_config = OpenMetadataJWTClientConfig(
        jwtToken="eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJlbWFpbCI6ImluZ2VzdGlvbi1ib3RAb3Blbm1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3NTQyMzE0NTMsImV4cCI6bnVsbH0.YigVaOPkDWTsbRUAcnE_oN2fvlf8dUk0hxr3JA5uLrsTtWQzinP4DA-R63wMo-LxUyToX_C9Cu8l_kXFvwY9YW-sOxBcBNz3qbR2B2-Psmy5S1XbliWUg9igyNa9scPSmY7H4sWu88PX06fIt1YdEfeDPLK0u3B70pFqTy2649ptIRVqmjhBoGNAnuRH29omOxnyR3xh13vfAkIM2VClkhmW_zHN3qWt9I1BZk2RLZQJ5DOgYcgj6Sy3RFucehKsGTB8gG7GnWOTvDIQWhwk-zCCa4MpfM1T9GUU0vBRUig-ghVBSGlyBLw4RFokQNr5-mhiIupVQteBhT7eGXVM3w"
    )

    # 2) Conexión al servidor con JWT
    server_config = OpenMetadataConnection(
        hostPort="http://openmetadata-server:8585/api",
        authProvider=AuthProvider.openmetadata,
        securityConfig=security_config,
    )
    metadata = OpenMetadata(server_config)

    # 3) Registrar servicio de pipelines (igual que antes) …
    service = PipelineService(
        name="SteamDataPipeline",
        serviceType=PipelineServiceType.CustomPipeline,
        connection=None,
    )
    existing = metadata.get_by_name(PipelineService, fqn="SteamDataPipeline")
    if existing:
        service.id = existing.id
    else:
        service = metadata.create_or_update(service)

    # 4) Registrar el pipeline
    pipeline_request = CreatePipelineRequest(
        name="LandingToTrusted",
        service=EntityReference(id=service.id, type="pipelineService"),
        description="Pipeline ETL: NDJSON desde landing_zone a MongoDB en trusted_zone usando Spark.",
    )
    created = metadata.create_or_update(pipeline_request)
    print(f"✅ Pipeline registrado con ID: {created.id}")