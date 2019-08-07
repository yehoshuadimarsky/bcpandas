
$ErrorActionPreference = 'Stop';

# get configs and auth
Write-Host "Getting Configs..."
$config = Get-Content -Raw -Path "./dist.json" | ConvertFrom-Json
$auth = Get-Content -Raw -Path "./creds.json" | ConvertFrom-Json

$url = "https://github.com/$($config.GH_user)/$($config.name)/archive/$($config.version).tar.gz"
$dest = "$ENV:USERPROFILE\Downloads\$($config.name)-$(Split-Path $url -Leaf)"
$condaEnv = "dist-env"


# Setup conda
conda create -n $condaEnv python -y
conda install -n $condaEnv -c conda-forge pygithub twine click jinja2 -y
# get path to env python
$envpath = ((conda info -e) -match $condaEnv ).Split(" ")[-1]

# PyPI
if (Test-Path "./dist") { Remove-Item "./dist" -Recurse; }
python .\setup.py sdist bdist_wheel

# add --repository-url https://test.pypi.org/legacy/  if to test.pypi.org
Start-Process "$envpath\python.exe" -ArgumentList "-m twine upload --verbose -u $($auth.pypi_username) -p $($auth.pypi_password) dist/*" -NoNewWindow -Wait 


# deploy to GitHub
Start-Process "$envpath\python.exe" -ArgumentList ".\dist.py github-release" -NoNewWindow -Wait 

# conda
# get sha256 of GitHub tar.gz
Write-Host "Downloading $($config.name) from $url"
(New-Object System.Net.WebClient).DownloadFile($url, $dest)
if (!(Test-Path $dest)) {
    Write-Host "Error, $($config.name) not found in $dest" -ForegroundColor Red
} else {
    Write-Host "$($config.name) downloaded successfully to $dest" -ForegroundColor Green
}
$hash = (certutil -hashfile $dest sha256 )[1]  # returns 3 rows, 2nd is hash

# render meta.yaml
Start-Process "$envpath\python.exe" -ArgumentList ".\dist.py render-conda --sha256 $hash" -NoNewWindow -Wait 


# TODO upload meta.yaml to conda-forge feedstock, create PR

# cleanup
Remove-Item -Path $dest 
conda remove -n $condaEnv --all -y
Remove-Item -Path $envpath -Recurse
