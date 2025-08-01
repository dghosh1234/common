Option Explicit

Dim tbl, col
Dim fso, file, filePath
Dim semVal, suggestion, lineOut

' File path on desktop
filePath = CreateObject("WScript.Shell").SpecialFolders("Desktop") & "\varchar2_semantics_report.csv"

Set fso = CreateObject("Scripting.FileSystemObject")
Set file = fso.CreateTextFile(filePath, True)

file.WriteLine "Table Name,Column Name,Data Type,Length,Semantics,Suggestion"

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        For Each col In tbl.Columns
            If InStr(UCase(col.DataType), "VARCHAR2") > 0 Then
                Dim dataTypeUpper, len
                dataTypeUpper = UCase(col.DataType)
                len = col.Length

                ' Get semantics from extended attribute (CHAR/BYTE/blank)
                On Error Resume Next
                semVal = col.GetExtendedAttribute("LengthSemantics")
                If Err.Number <> 0 Then semVal = "" : Err.Clear
                On Error GoTo 0

                semVal = UCase(Trim(semVal))

                ' ✅ Include only if semantics is BYTE or blank AND not already using VARCHAR2(n CHAR)
                If (semVal = "BYTE" Or semVal = "") And InStr(dataTypeUpper, "CHAR") = 0 Then
                    suggestion = "Change to VARCHAR2(" & len & " CHAR) semantics"
                    lineOut = """" & tbl.Code & """,""" & col.Code & """,""" & col.DataType & """,""" & len & """,""" & semVal & """,""" & suggestion & """"
                    file.WriteLine lineOut
                End If
            End If
        Next
    End If
Next

file.Close

MsgBox "✅ Report generated successfully at: " & filePath
