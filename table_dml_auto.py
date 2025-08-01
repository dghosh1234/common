Option Explicit

Dim tbl, col
Dim fso, file, filePath
Dim semantics, suggestion
Dim outputLine, cleanLength

' Output file path on Desktop
filePath = CreateObject("WScript.Shell").SpecialFolders("Desktop") & "\varchar2_semantics_report.csv"

Set fso = CreateObject("Scripting.FileSystemObject")
Set file = fso.CreateTextFile(filePath, True)

' CSV header
file.WriteLine "Table Name,Column Name,Data Type,Length,Semantics,Suggestion"

' Loop through all tables
For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        For Each col In tbl.Columns
            If InStr(UCase(col.DataType), "VARCHAR2") > 0 Then
                On Error Resume Next
                semantics = col.GetExtendedAttribute("LengthSemantics")
                If Err.Number <> 0 Then
                    semantics = ""
                    Err.Clear
                End If
                On Error GoTo 0

                ' Only include if semantics is NOT CHAR
                If Trim(UCase(semantics)) <> "CHAR" Then
                    cleanLength = col.Length
                    suggestion = "Change to VARCHAR2(" & cleanLength & " CHAR) semantics"

                    outputLine = """" & tbl.Code & """,""" & col.Code & """,""" & col.DataType & """,""" & col.Length & """,""" & semantics & """,""" & suggestion & """"
                    file.WriteLine outputLine
                End If
            End If
        Next
    End If
Next

file.Close

MsgBox "✅ CSV report saved to Desktop as 'varchar2_semantics_report.csv' (only discrepancies shown)", vbInformation, "VARCHAR2 CHAR Semantics Report"
