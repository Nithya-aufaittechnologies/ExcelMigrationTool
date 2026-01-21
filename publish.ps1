# PowerShell script to publish and prepare the application for IIS deployment
# Run this script from the project root directory

param(
    [string]$PublishPath = "C:\Publish\ExcelMigrationTool",
    [string]$Configuration = "Release"
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Excel Migration Tool - IIS Deployment" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if .NET SDK is installed
Write-Host "Checking .NET SDK installation..." -ForegroundColor Yellow
try {
    $dotnetVersion = dotnet --version
    Write-Host "✓ .NET SDK version: $dotnetVersion" -ForegroundColor Green
} catch {
    Write-Host "✗ .NET SDK not found. Please install .NET 8.0 SDK." -ForegroundColor Red
    exit 1
}

# Clean previous publish
if (Test-Path $PublishPath) {
    Write-Host "Cleaning previous publish folder..." -ForegroundColor Yellow
    Remove-Item -Path $PublishPath -Recurse -Force
}

# Create publish directory
Write-Host "Creating publish directory: $PublishPath" -ForegroundColor Yellow
New-Item -ItemType Directory -Path $PublishPath -Force | Out-Null

# Publish the application
Write-Host "Publishing application..." -ForegroundColor Yellow
Write-Host "Configuration: $Configuration" -ForegroundColor Gray
Write-Host "Output: $PublishPath" -ForegroundColor Gray
Write-Host ""

$publishResult = dotnet publish -c $Configuration -o $PublishPath --self-contained false

if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ Publishing failed!" -ForegroundColor Red
    exit 1
}

Write-Host "✓ Application published successfully!" -ForegroundColor Green
Write-Host ""

# Verify critical files
Write-Host "Verifying published files..." -ForegroundColor Yellow
$requiredFiles = @(
    "ExcelMigrationTool.dll",
    "web.config",
    "appsettings.json"
)

$allFilesExist = $true
foreach ($file in $requiredFiles) {
    $filePath = Join-Path $PublishPath $file
    if (Test-Path $filePath) {
        Write-Host "  ✓ $file" -ForegroundColor Green
    } else {
        Write-Host "  ✗ $file (MISSING!)" -ForegroundColor Red
        $allFilesExist = $false
    }
}

if (-not $allFilesExist) {
    Write-Host ""
    Write-Host "✗ Some required files are missing!" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Publish completed successfully!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Review and update appsettings.json with production connection string" -ForegroundColor White
Write-Host "2. Create IIS Application Pool (No Managed Code, Integrated Pipeline)" -ForegroundColor White
Write-Host "3. Create IIS Website/Application pointing to: $PublishPath" -ForegroundColor White
Write-Host "4. Set folder permissions for IIS App Pool identity" -ForegroundColor White
Write-Host "5. Create logs folder and grant write permissions" -ForegroundColor White
Write-Host ""
Write-Host "See DEPLOYMENT.md for detailed instructions." -ForegroundColor Cyan
Write-Host ""

