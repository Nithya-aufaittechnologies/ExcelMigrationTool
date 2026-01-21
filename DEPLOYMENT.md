# IIS Deployment Guide for Excel Migration Tool

This guide will help you deploy the Excel Migration Tool to IIS on Windows Server.

## Prerequisites

### 1. Install .NET 8.0 Hosting Bundle
Download and install the **.NET 8.0 Hosting Bundle** from:
- https://dotnet.microsoft.com/download/dotnet/8.0
- Select "Hosting Bundle" (includes .NET Runtime and ASP.NET Core Module)

**Important:** Install the Hosting Bundle, NOT just the Runtime.

### 2. Install IIS Features
Ensure the following IIS features are installed:
- IIS (Internet Information Services)
- ASP.NET Core Module V2
- .NET Extensibility
- ISAPI Extensions
- ISAPI Filters

You can install these via PowerShell (Run as Administrator):
```powershell
Enable-WindowsOptionalFeature -Online -FeatureName IIS-WebServerRole
Enable-WindowsOptionalFeature -Online -FeatureName IIS-WebServer
Enable-WindowsOptionalFeature -Online -FeatureName IIS-CommonHttpFeatures
Enable-WindowsOptionalFeature -Online -FeatureName IIS-HttpErrors
Enable-WindowsOptionalFeature -Online -FeatureName IIS-ApplicationInit
Enable-WindowsOptionalFeature -Online -FeatureName IIS-HealthAndDiagnostics
Enable-WindowsOptionalFeature -Online -FeatureName IIS-HttpLogging
Enable-WindowsOptionalFeature -Online -FeatureName IIS-Security
Enable-WindowsOptionalFeature -Online -FeatureName IIS-RequestFiltering
Enable-WindowsOptionalFeature -Online -FeatureName IIS-Performance
Enable-WindowsOptionalFeature -Online -FeatureName IIS-HttpCompressionStatic
Enable-WindowsOptionalFeature -Online -FeatureName IIS-WebServerManagementTools
Enable-WindowsOptionalFeature -Online -FeatureName IIS-ManagementConsole
```

## Publishing the Application

### Option 1: Publish from Visual Studio

1. Right-click on the project in Solution Explorer
2. Select **Publish**
3. Choose **Folder** as the publish target
4. Click **Publish**
5. The published files will be in `bin\Release\net8.0\publish\`

### Option 2: Publish from Command Line

Open PowerShell or Command Prompt in the project directory and run:

```powershell
dotnet publish -c Release -o "C:\Publish\ExcelMigrationTool"
```

This will create a publish folder with all necessary files.

## IIS Configuration

### 1. Create Application Pool

1. Open **IIS Manager**
2. Right-click on **Application Pools** → **Add Application Pool**
3. Configure:
   - **Name:** `ExcelMigrationToolAppPool`
   - **.NET CLR Version:** **No Managed Code** (Important for .NET Core/5+/6+/7+/8+)
   - **Managed Pipeline Mode:** **Integrated**
4. Click **OK**

5. Right-click the new App Pool → **Advanced Settings**:
   - **Start Mode:** `AlwaysRunning` (optional, for better performance)
   - **Identity:** Use `ApplicationPoolIdentity` or a specific service account

### 2. Create IIS Website/Application

1. In IIS Manager, right-click **Sites** → **Add Website**
2. Configure:
   - **Site name:** `ExcelMigrationTool`
   - **Application pool:** Select `ExcelMigrationToolAppPool`
   - **Physical path:** Point to your published folder (e.g., `C:\Publish\ExcelMigrationTool`)
   - **Binding:**
     - **Type:** `http` or `https`
     - **IP address:** `All Unassigned` or specific IP
     - **Port:** `80` (http) or `443` (https)
     - **Host name:** (optional) e.g., `excelmigration.yourdomain.com`
3. Click **OK**

### 3. Configure Application Settings

1. Select your website in IIS Manager
2. Double-click **Configuration Editor**
3. Navigate to `system.webServer/aspNetCore`
4. Verify settings:
   - `processPath`: `dotnet`
   - `arguments`: `.\ExcelMigrationTool.dll`
   - `hostingModel`: `inprocess`

### 4. Set Folder Permissions

Grant the Application Pool identity read/execute permissions on the application folder:

1. Right-click the published folder → **Properties** → **Security** tab
2. Click **Edit** → **Add**
3. Enter: `IIS AppPool\ExcelMigrationToolAppPool`
4. Grant: **Read & Execute**, **List folder contents**, **Read**
5. Click **OK**

### 5. Create Logs Folder (Optional but Recommended)

Create a `logs` folder in your published directory for stdout logging:
```powershell
New-Item -ItemType Directory -Path "C:\Publish\ExcelMigrationTool\logs"
```

Grant write permissions to the App Pool identity on the logs folder.

## Configuration Files

### Update appsettings.json

Before deploying, ensure `appsettings.json` has the correct production connection string:

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Your Production Connection String"
  }
}
```

**Security Note:** Consider using:
- Environment variables
- Azure Key Vault
- IIS Application Settings (via Configuration Editor)
- User Secrets (for development only)

### Environment Variables

You can set environment variables in IIS:
1. Select the Application Pool
2. Click **Advanced Settings**
3. Under **Environment Variables**, add:
   - `ASPNETCORE_ENVIRONMENT` = `Production`
   - `ASPNETCORE_URLS` = (if needed)

## Testing the Deployment

1. Open a browser and navigate to: `http://your-server/swagger` or `http://your-server/`
2. You should see the Swagger UI
3. Test the API endpoint: `POST /api/excel/upload`

## Troubleshooting

### Common Issues

1. **HTTP Error 500.30 - In-Process Start Failure**
   - Check that .NET 8.0 Hosting Bundle is installed
   - Verify Application Pool is set to "No Managed Code"
   - Check event logs for detailed errors

2. **HTTP Error 500.0 - ANCM In-Process Handler Load Failure**
   - Verify `web.config` exists in the published folder
   - Check that `ExcelMigrationTool.dll` exists
   - Ensure all dependencies are published

3. **File Upload Size Limits**
   - Default limit is 100 MB (configured in `web.config`)
   - To change, modify `maxAllowedContentLength` in `web.config`

4. **Connection String Issues**
   - Verify SQL Server is accessible from the IIS server
   - Check firewall rules
   - Verify connection string format

5. **Permission Errors**
   - Ensure App Pool identity has read/execute permissions
   - Check logs folder has write permissions
   - Verify SQL Server connection permissions

### Viewing Logs

1. **IIS Logs:** `C:\inetpub\logs\LogFiles\`
2. **Application Logs:** `C:\Publish\ExcelMigrationTool\logs\stdout_*.log` (if enabled)
3. **Windows Event Viewer:** 
   - Windows Logs → Application
   - Applications and Services Logs → Microsoft → Windows → IIS-Configuration

### Enable Detailed Error Messages (Development Only)

In `web.config`, temporarily set:
```xml
<aspNetCore ... stdoutLogEnabled="true" ...>
```

## Security Recommendations

1. **Use HTTPS:** Configure SSL certificate for production
2. **Disable Swagger in Production:** Modify `Program.cs` to only enable Swagger in Development
3. **Use Windows Authentication:** If needed, configure in IIS
4. **Restrict Access:** Use IP restrictions or firewall rules
5. **Connection String Security:** Use encrypted connection strings or Azure Key Vault

## Performance Tuning

1. **Application Pool Settings:**
   - Set `Start Mode` to `AlwaysRunning`
   - Configure `Idle Timeout` as needed
   - Set appropriate `Maximum Worker Processes` for load balancing

2. **Request Limits:**
   - Adjust `maxAllowedContentLength` based on your needs
   - Configure timeout settings in IIS

## Maintenance

1. **Updates:** 
   - Publish new version to a staging folder
   - Test thoroughly
   - Update IIS physical path or swap folders

2. **Backups:**
   - Backup `appsettings.json`
   - Backup published application folder
   - Document any custom IIS configurations

## Support

For issues or questions, check:
- Application Event Logs
- IIS Logs
- Application stdout logs (if enabled)

