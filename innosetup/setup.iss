#define MyAppName "Data Version Control"
; FIXME: Hardcoded version is not nice, but will do for now.
#define MyAppVersion "0.8.1"
#define MyAppPublisher "Dmitry Petrov"
#define MyAppURL "https://dataversioncontrol.com/"
#define MyAppExeName "dvc.exe"

[Setup]
AppId={{8258CE8A-110E-4E0D-AE60-FEE00B15F041}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
;AppVerName={#MyAppName} {#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={pf}\{#MyAppName}
DefaultGroupName={#MyAppName}
AllowNoIcons=yes
LicenseFile=..\LICENSE
OutputBaseFilename=dvc-{#MyAppVersion}
Compression=lzma
SolidCompression=yes
OutputDir=..\
ChangesEnvironment=yes

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Files]
Source: "..\nuitka.build\dvc.dist\*"; DestDir: "{app}"; Flags: ignoreversion
; NOTE: Don't use "Flags: ignoreversion" on any shared system files

[Tasks]
Name: modifypath; Description: Adds dvc's application directory to environmental path;

[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent

[Code]
const
	ModPathName = 'modifypath';
	ModPathType = 'user';

function ModPathDir(): TArrayOfString;
begin
	setArrayLength(Result, 1)
	Result[0] := ExpandConstant('{app}');
end;
#include "modpath.iss"
