$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$scratchRoot = [System.IO.Path]::GetTempPath()
$scratch = Join-Path $scratchRoot ("raftkv-tests-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $scratch | Out-Null

try {
    $javaHome = if ($env:JAVA_HOME) { $env:JAVA_HOME } else { throw "JAVA_HOME is required" }
    $javac = Join-Path $javaHome "bin\javac.exe"
    $java = Join-Path $javaHome "bin\java.exe"
    $classes = Join-Path $scratch "classes"
    New-Item -ItemType Directory -Path $classes | Out-Null

    $sources = Get-ChildItem `
        (Join-Path $repoRoot "java\src\main\java"), `
        (Join-Path $repoRoot "java\src\test\java") `
        -Recurse -Filter "*.java" | Select-Object -ExpandProperty FullName
    & $javac --release 17 -Xlint:all -Werror -d $classes $sources
    & $java -ea -cp $classes com.example.raftkv.RaftCoreTest
    & $java -ea -cp $classes com.example.raftkv.ChaosHarness 1000

    if ($env:RAFTKV_PYTHON) {
        $pythonExe = $env:RAFTKV_PYTHON
        $pythonArgs = @()
    }
    elseif ($python = Get-Command python -ErrorAction SilentlyContinue) {
        $pythonExe = $python.Source
        $pythonArgs = @()
    }
    elseif ($py = Get-Command py -ErrorAction SilentlyContinue) {
        $pythonExe = $py.Source
        $pythonArgs = @("-3")
    }
    else {
        throw "Python 3 is required (or set RAFTKV_PYTHON)"
    }
    & $pythonExe @pythonArgs -m unittest discover -s (Join-Path $repoRoot "python") -p "test_*.py" -v
}
finally {
    $resolvedScratch = [System.IO.Path]::GetFullPath($scratch)
    $resolvedTemp = [System.IO.Path]::GetFullPath($scratchRoot)
    if ($resolvedScratch.StartsWith($resolvedTemp) -and (Split-Path $resolvedScratch -Leaf).StartsWith("raftkv-tests-")) {
        Remove-Item -LiteralPath $resolvedScratch -Recurse -Force -ErrorAction SilentlyContinue
    }
}
