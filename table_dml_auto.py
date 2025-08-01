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

                ' Read semantics (may be empty or missing)
                On Error Resume Next
                semVal = col.GetExtendedAttribute("LengthSemantics")
                If Err.Number <> 0 Then semVal = "" : Err.Clear
                On Error GoTo 0

                semVal = UCase(Trim(semVal))

                ' 🟢 Condition: not already using CHAR semantics
                If InStr(dataTypeUpper, "CHAR") = 0 And semVal <> "CHAR" Then
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



===============


Option Explicit

Dim tbl, col
Dim fso, file, filePath
Dim semVal, suggestion, lineOut, debugNote

' Output file path on desktop
filePath = CreateObject("WScript.Shell").SpecialFolders("Desktop") & "\varchar2_semantics_report_debug.csv"

Set fso = CreateObject("Scripting.FileSystemObject")
Set file = fso.CreateTextFile(filePath, True)

file.WriteLine "Table Name,Column Name,Data Type,Length,Semantics,Suggestion,Debug Note"

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        For Each col In tbl.Columns
            Dim dataTypeUpper, len
            dataTypeUpper = UCase(Trim(col.DataType))
            len = col.Length

            ' Read semantics, default to blank if missing
            On Error Resume Next
            semVal = col.GetExtendedAttribute("LengthSemantics")
            If Err.Number <> 0 Then semVal = "" : Err.Clear
            On Error GoTo 0
            semVal = UCase(Trim(semVal))

            ' Initialize default values
            suggestion = ""
            debugNote = ""

            If InStr(dataTypeUpper, "VARCHAR2") = 0 Then
                debugNote = "Not a VARCHAR2 column"
            ElseIf InStr(dataTypeUpper, "CHAR") > 0 Then
                debugNote = "Already using CHAR in data type"
            ElseIf semVal = "CHAR" Then
                debugNote = "Already using CHAR semantics"
            Else
                suggestion = "Change to VARCHAR2(" & len & " CHAR) semantics"
                debugNote = "Match found"
            End If

            ' Write line for all columns
            lineOut = """" & tbl.Code & """,""" & col.Code & """,""" & col.DataType & """,""" & len & """,""" & semVal & """,""" & suggestion & """,""" & debugNote & """"
            file.WriteLine lineOut
        Next
    End If
Next

file.Close

MsgBox "✅ Debug report generated: " & filePath
