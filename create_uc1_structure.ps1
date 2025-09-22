# ================================================
# GeoRetail UC1: Explorer Mode - Create Structure
# PowerShell Version for Windows
# ================================================

Write-Host "================================================" -ForegroundColor Blue
Write-Host "   GeoRetail UC1: Structure Creation" -ForegroundColor Blue  
Write-Host "================================================`n" -ForegroundColor Blue

# Check if we're in the right directory
if (-not (Test-Path "src")) {
    Write-Host "Error: Please run this script from the georetail/ root directory" -ForegroundColor Red
    exit 1
}

$baseDir = Get-Location
Write-Host "Creating structure in: $baseDir" -ForegroundColor Yellow
Write-Host ("=" * 50)

# Backend structure
$backendStructure = @{
    "src\api\v2\territories" = @(
        "__init__.py",
        "router.py",
        "services.py",
        "schemas.py",
        "queries.py"
    )
    "src\api\v2\admin" = @(
        "__init__.py",
        "router.py",
        "services.py",
        "schemas.py"
    )
    "src\database" = @(
        "__init__.py",
        "connections.py"
    )
    "src\database\queries" = @(
        "__init__.py",
        "admin_queries.py",
        "h3_queries.py"
    )
}

# Frontend structure
$frontendStructure = @{
    "frontend\public" = @(
        "index.html",
        "favicon.ico"
    )
    "frontend\src\components\Explorer" = @(
        "ExplorerView.jsx",
        "HoverCard.jsx",
        "SidePanel.jsx",
        "BivariateLegend.jsx",
        "index.js"
    )
    "frontend\src\components\Map" = @(
        "DeckGLMap.jsx"
    )
    "frontend\src\components\Map\layers" = @(
        "AdminUnitsLayer.js",
        "H3HexagonLayer.js",
        "index.js"
    )
    "frontend\src\services\api" = @(
        "auth.js",
        "territories.js",
        "config.js",
        "index.js"
    )
    "frontend\src\store" = @(
        "explorerStore.js",
        "authStore.js",
        "index.js"
    )
    "frontend\src\styles" = @(
        "globals.css"
    )
    "frontend\src\styles\components" = @(
        "Explorer.module.css",
        "Map.module.css"
    )
    "frontend\src\utils" = @(
        "bivariate.js",
        "formatters.js",
        "constants.js"
    )
}

# Create backend folders
Write-Host "Backend structure:" -ForegroundColor Green
foreach ($path in $backendStructure.Keys) {
    $fullPath = Join-Path $baseDir $path
    
    # Create directory
    if (-not (Test-Path $fullPath)) {
        New-Item -ItemType Directory -Path $fullPath -Force | Out-Null
        Write-Host "Created folder: $path\" -ForegroundColor Green
    } else {
        Write-Host "Exists: $path\" -ForegroundColor Yellow
    }
    
    # Create files
    foreach ($file in $backendStructure[$path]) {
        $filePath = Join-Path $fullPath $file
        if (-not (Test-Path $filePath)) {
            if ($file -eq "__init__.py") {
                Set-Content -Path $filePath -Value '"""Package initialization"""'
            } else {
                $moduleName = $file.Replace('.py', '')
                $content = @"
"""
$moduleName module for $path
"""

# TODO: Implement $moduleName
"@
                Set-Content -Path $filePath -Value $content
            }
            Write-Host "Created: $file" -ForegroundColor Cyan
        } else {
            Write-Host "Exists: $file" -ForegroundColor Gray
        }
    }
}

# Create frontend folders
Write-Host "Frontend structure:" -ForegroundColor Green
foreach ($path in $frontendStructure.Keys) {
    $fullPath = Join-Path $baseDir $path
    
    # Create directory
    if (-not (Test-Path $fullPath)) {
        New-Item -ItemType Directory -Path $fullPath -Force | Out-Null
        Write-Host "Created folder: $path\" -ForegroundColor Green
    } else {
        Write-Host "Exists: $path\" -ForegroundColor Yellow
    }
    
    # Create files
    foreach ($file in $frontendStructure[$path]) {
        $filePath = Join-Path $fullPath $file
        if (-not (Test-Path $filePath)) {
            $content = ""
            
            if ($file.EndsWith('.jsx')) {
                $componentName = $file.Replace('.jsx', '')
                $content = @"
import React from 'react';

const $componentName = () => {
  return (
    <div>
      $componentName Component
    </div>
  );
};

export default $componentName;
"@
            }
            elseif ($file.EndsWith('.js')) {
                if ($file -eq 'index.js') {
                    $content = "// Export all components from this module`n"
                } else {
                    $moduleName = $file.Replace('.js', '')
                    $content = "// $moduleName module`n`nexport default {};`n"
                }
            }
            elseif ($file.EndsWith('.css')) {
                $styleName = $file.Replace('.css', '').Replace('.module', '')
                $content = "/* Styles for $styleName */`n"
            }
            elseif ($file -eq 'index.html') {
                $content = @"
<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>GeoRetail Explorer</title>
</head>
<body>
  <div id="root"></div>
</body>
</html>
"@
            }
            
            Set-Content -Path $filePath -Value $content -Encoding UTF8
            Write-Host "Created: $file" -ForegroundColor Cyan
        } else {
            Write-Host "Exists: $file" -ForegroundColor Gray
        }
    }
}

# Create main.py if not exists
$mainFile = Join-Path $baseDir "src\main.py"
if (-not (Test-Path $mainFile)) {
    Write-Host "Creating main.py..." -ForegroundColor Yellow
    $mainContent = @"
"""
GeoRetail API v2 Main Application
"""

# TODO: Add FastAPI app initialization
"@
    Set-Content -Path $mainFile -Value $mainContent
    Write-Host "Created src\main.py" -ForegroundColor Green
}

# Create frontend config files
Write-Host "Frontend configurations:" -ForegroundColor Yellow

# package.json
$packageJsonPath = Join-Path $baseDir "frontend\package.json"
if (-not (Test-Path $packageJsonPath)) {
    $packageJson = @{
        name = "georetail-frontend"
        version = "1.0.0"
        type = "module"
        scripts = @{
            dev = "vite"
            build = "vite build"
            preview = "vite preview"
        }
        dependencies = @{
            "react" = "^18.2.0"
            "react-dom" = "^18.2.0"
            "deck.gl" = "^8.9.0"
            "@deck.gl/layers" = "^8.9.0"
            "@deck.gl/geo-layers" = "^8.9.0"
            "mapbox-gl" = "^3.0.0"
            "zustand" = "^4.4.0"
            "axios" = "^1.6.0"
        }
        devDependencies = @{
            "@vitejs/plugin-react" = "^4.2.0"
            "vite" = "^5.0.0"
            "tailwindcss" = "^3.4.0"
            "autoprefixer" = "^10.4.0"
            "postcss" = "^8.4.0"
        }
    }
    $packageJson | ConvertTo-Json -Depth 10 | Set-Content -Path $packageJsonPath
    Write-Host "Created: frontend\package.json" -ForegroundColor Green
}

# vite.config.js
$viteConfigPath = Join-Path $baseDir "frontend\vite.config.js"
if (-not (Test-Path $viteConfigPath)) {
    $viteConfig = @"
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true
      }
    }
  }
});
"@
    Set-Content -Path $viteConfigPath -Value $viteConfig
    Write-Host "Created: frontend\vite.config.js" -ForegroundColor Green
}

# Create .env.example
$envExamplePath = Join-Path $baseDir ".env.example"
if (-not (Test-Path $envExamplePath)) {
    $envContent = @"
# Database
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=georetail
POSTGRES_USER=georetail_user
POSTGRES_PASSWORD=georetail_secure_2024

# ClickHouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=32769
CLICKHOUSE_USER=webuser
CLICKHOUSE_PASSWORD=password123

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=redis_secure_2024

# JWT
JWT_SECRET_KEY=your-secret-key-here-min-32-chars
JWT_ALGORITHM=HS256

# Application
ENVIRONMENT=development
DEBUG=True
PORT=8000
"@
    Set-Content -Path $envExamplePath -Value $envContent
    Write-Host "Created .env.example" -ForegroundColor Green
}

# Create logs folder
$logsPath = Join-Path $baseDir "logs"
if (-not (Test-Path $logsPath)) {
    New-Item -ItemType Directory -Path $logsPath -Force | Out-Null
    Write-Host "Created logs\ folder" -ForegroundColor Green
}

Write-Host "" ("=" * 50)
Write-Host "Structure for UC1 successfully created!" -ForegroundColor Green
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Copy created files (router.py, schemas.py, etc.) to folders" -ForegroundColor White
Write-Host "2. Install Python dependencies: pip install -r requirements.txt" -ForegroundColor White
Write-Host "3. Install Frontend dependencies: cd frontend; npm install" -ForegroundColor White
Write-Host "4. Create .env file based on .env.example" -ForegroundColor White
Write-Host "5. Run backend: python src\main.py" -ForegroundColor White
Write-Host "6. Run frontend: cd frontend; npm run dev" -ForegroundColor White