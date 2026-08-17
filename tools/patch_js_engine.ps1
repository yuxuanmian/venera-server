$ErrorActionPreference = 'Stop'
$path = 'D:\project\tool\venera-project\venera\lib\foundation\js_engine.dart'
$text = [System.IO.File]::ReadAllText($path)

$pattern = "(?m)([ \t]*case 'http':\r?\n)[ \t]*return _http\(Map\.from\(message\)\);"

$evaluator = {
    param($m)
    $head = $m.Groups[1].Value
    return $head +
        "            final fix = httpFixture;`n" +
        "            if (fix != null) {`n" +
        "              final res = fix.handle(Map.from(message));`n" +
        "              if (res != null) return res;`n" +
        "            }`n" +
        "            return _http(Map.from(message));"
}

if ($text -match $pattern) {
    $new = [regex]::Replace($text, $pattern, $evaluator)
    [System.IO.File]::WriteAllText($path, $new)
    Write-Output 'patched'
} else {
    Write-Output 'pattern not found - no change'
}
