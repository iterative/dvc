procedure AddSymLink();
var
	ErrorCode: Integer;
	SRCdir: String;
begin
	SRCdir := ExpandConstant('{app}');
	if isUninstaller() = false then
		ShellExec('', 'powershell.exe', '-noninteractive -windowstyle hidden -executionpolicy bypass -File "' + SRCdir + '\addSymLinkPermissions.ps1" -mytype ' + SymLinkType, '', SW_HIDE, ewWaitUntilTerminated, ErrorCode);
end;
