Option Explicit

Dim tbl, col
Dim fso, file, filePath
Dim semantics, suggestion
Dim outputLine

' Output file to Desktop
filePath = CreateObject("WScript.Shell").SpecialFolders("Desktop") & "\varchar2_semantics_report.csv"

Set fso = CreateObject("Scripting.FileSystemObject")
Set file = fso.CreateTextFile(filePath, True)

' CSV header
file.WriteLine "Table Name,Column Name,Data Type,Length,Semantics,Suggestion"

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        For Each col In tbl.Columns
            ' Check it's a VARCHAR2
            If InStr(UCase(col.DataType), "VARCHAR2") > 0 Then
                Dim dataTypeUpper
                dataTypeUpper = UCase(col.DataType)

                ' Get semantics safely
                On Error Resume Next
                semantics = col.GetExtendedAttribute("LengthSemantics")
                If Err.Number <> 0 Then semantics = "" : Err.Clear
                On Error GoTo 0

                semantics = UCase(Trim(semantics))

                ' ✅ Only include if:
                ' - DataType doesn't mention CHAR (e.g., VARCHAR2(30))
                ' - Semantics is not CHAR (i.e., it's BYTE or blank)
                If InStr(dataTypeUpper, "CHAR") = 0 And semantics <> "CHAR" Then
                    suggestion = "Change to VARCHAR2(" & col.Length & " CHAR) semantics"
                    outputLine = """" & tbl.Code & """,""" & col.Code & """,""" & col.DataType & """,""" & col.Length & """,""" & semantics & """,""" & suggestion & """"
                    file.WriteLine outputLine
                End If
            End If
        Next
    End If
Next

file.Close

MsgBox "✅ Report saved to: " & filePath, vbInformation, "VARCHAR2 BYTE → CHAR Fix Needed"
