Option Explicit

Dim tbl, col
Dim fso, file, filePath
Dim semantics, suggestion
Dim outputLine

' Output file on Desktop
filePath = CreateObject("WScript.Shell").SpecialFolders("Desktop") & "\varchar2_semantics_report.csv"

Set fso = CreateObject("Scripting.FileSystemObject")
Set file = fso.CreateTextFile(filePath, True)

' CSV Header
file.WriteLine "Table Name,Column Name,Data Type,Length,Semantics,Suggestion"

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        For Each col In tbl.Columns
            If InStr(UCase(col.DataType), "VARCHAR2") > 0 Then
                Dim dataTypeUpper, len, semVal
                dataTypeUpper = UCase(col.DataType)
                len = col.Length

                ' Get semantics safely (can be missing/null)
                On Error Resume Next
                semVal = col.GetExtendedAttribute("LengthSemantics")
                If Err.Number <> 0 Then semVal = "" : Err.Clear
                On Error GoTo 0

                semVal = UCase(Trim(semVal))

                ' 🔍 Logic: Include if:
                ' 1. DataType does NOT contain CHAR
                ' 2. Semantics is not CHAR (includes blank or BYTE → default BYTE)
                If InStr(dataTypeUpper, "CHAR") = 0 And semVal <> "CHAR" Then
                    suggestion = "Change to VARCHAR2(" & len & " CHAR) semantics"
                    outputLine = """" & tbl.Code & """,""" & col.Code & """,""" & col.DataType & """,""" & len & """,""" & semVal & """,""" & suggestion & """"
                    file.WriteLine outputLine
                End If
            End If
        Next
    End If
Next

file.Close

MsgBox "✅ Report saved to: " & filePath, vbInformation, "VARCHAR2 BYTE → CHAR Fix Needed"
