Option Explicit

Dim tbl, col
Dim fso, file, filePath
Dim semVal, suggestion, lineOut
Dim dataTypeUpper, len

' Output file on desktop
filePath = CreateObject("WScript.Shell").SpecialFolders("Desktop") & "\VARCHAR2_Char_Semantics_Report.csv"

Set fso = CreateObject("Scripting.FileSystemObject")
Set file = fso.CreateTextFile(filePath, True)

file.WriteLine "Table Name,Column Name,Data Type,Length,Semantics,Suggestion"

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        For Each col In tbl.Columns
            dataTypeUpper = UCase(col.DataType)

            ' Only interested in VARCHAR2 data type (not already CHAR-based)
            If InStr(dataTypeUpper, "VARCHAR2") > 0 And InStr(dataTypeUpper, "CHAR") = 0 Then
                len = col.Length

                ' Get semantics safely
                On Error Resume Next
                semVal = col.GetExtendedAttribute("LengthSemantics")
                If Err.Number <> 0 Then semVal = "" : Err.Clear
                On Error GoTo 0

                semVal = UCase(Trim(semVal))

                ' Only include columns NOT already using CHAR semantics
                If semVal <> "CHAR" Then
                    suggestion = "Change to VARCHAR2(" & len & " CHAR)"
                    lineOut = """" & tbl.Code & """,""" & col.Code & """,""" & col.DataType & """,""" & len & """,""" & semVal & """,""" & suggestion & """"
                    file.WriteLine lineOut
                End If
            End If
        Next
    End If
Next

file.Close

MsgBox "✅ VARCHAR2 CHAR Semantics report generated at:" & vbCrLf & filePath, vbInformation, "Scan Complete"
