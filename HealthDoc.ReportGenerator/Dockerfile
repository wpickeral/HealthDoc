# Build context must be the repo root so both projects are available:
#   docker build -f HealthDoc.ReportGenerator/Dockerfile -t healthdoc-report-generator:latest .

# Stage 1: build
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY HealthDoc.Models/ HealthDoc.Models/
COPY HealthDoc.ReportGenerator/ HealthDoc.ReportGenerator/
RUN dotnet publish HealthDoc.ReportGenerator/HealthDoc.ReportGenerator.csproj -c Release -o /app

# Stage 2: runtime — no SDK, smaller image
FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "HealthDoc.ReportGenerator.dll"]
