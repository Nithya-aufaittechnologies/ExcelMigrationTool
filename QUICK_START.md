# Quick Start Guide - IIS Deployment

## Prerequisites Checklist

- [ ] Windows Server with IIS installed
- [ ] .NET 8.0 Hosting Bundle installed (download from https://dotnet.microsoft.com/download/dotnet/8.0)
- [ ] SQL Server accessible from IIS server
- [ ] Administrator access to IIS server

## Quick Deployment Steps

### Step 1: Publish the Application

**Option A: Using PowerShell Script (Recommended)**
```powershell
.\publish.ps1 -PublishPath "C:\Publish\ExcelMigrationTool"
```

**Option B: Using Visual Studio**
1. Right-click project → Publish
2. Choose Folder → Publish

**Option C: Using Command Line**
```powershell
dotnet publish -c Release -o "C:\Publish\ExcelMigrationTool"
```

### Step 2: Update Configuration

Edit `C:\Publish\ExcelMigrationTool\appsettings.json` and update the connection string:
```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Your Production Connection String"
  }
}
```

### Step 3: Configure IIS

**Option A: Using PowerShell Script (Recommended - Run as Administrator)**
```powershell
.\setup-iis.ps1 -PhysicalPath "C:\Publish\ExcelMigrationTool" -Port 80
```

**Option B: Manual Configuration**
1. Open IIS Manager
2. Create Application Pool:
   - Name: `ExcelMigrationToolAppPool`
   - .NET CLR Version: **No Managed Code**
   - Pipeline: **Integrated**
3. Create Website:
   - Name: `ExcelMigrationTool`
   - Physical Path: `C:\Publish\ExcelMigrationTool`
   - Application Pool: `ExcelMigrationToolAppPool`
   - Port: `80`
4. Set Permissions:
   - Grant `IIS AppPool\ExcelMigrationToolAppPool` Read/Execute on application folder
   - Create `logs` folder and grant Write permissions

### Step 4: Test

Open browser and navigate to:
- `http://your-server/` (Swagger UI - Development only)
- `http://your-server/api/excel/upload` (API endpoint)

## Common Commands

### Check Application Status
```powershell
Get-WebAppPoolState -Name "ExcelMigrationToolAppPool"
Get-WebsiteState -Name "ExcelMigrationTool"
```

### Restart Application
```powershell
Restart-WebAppPool -Name "ExcelMigrationToolAppPool"
Restart-Website -Name "ExcelMigrationTool"
```

### View Logs
- IIS Logs: `C:\inetpub\logs\LogFiles\`
- Application Logs: `C:\Publish\ExcelMigrationTool\logs\` (if enabled)

## Troubleshooting

**500.30 Error:** Install .NET 8.0 Hosting Bundle
**500.0 Error:** Check web.config and ensure all DLLs are published
**Permission Errors:** Verify App Pool identity has proper permissions

For detailed troubleshooting, see `DEPLOYMENT.md`

