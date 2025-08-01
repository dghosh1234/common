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

                ' Normalize values
                semantics = Trim(UCase(semantics))
                
                ' Only generate row if semantics is not CHAR and DataType does NOT already contain CHAR
                If semantics <> "CHAR" And InStr(UCase(col.DataType), "CHAR") = 0 Then
                    suggestion = "Change to VARCHAR2(" & col.Length & " CHAR) semantics"

                    outputLine = """" & tbl.Code & """,""" & col.Code & """,""" & col.DataType & """,""" & col.Length & """,""" & semantics & """,""" & suggestion & """"
                    file.WriteLine outputLine
                End If
            End If
        Next
    End If
Next


Option Explicit

Dim tbl, col
Dim fso, file, filePath
Dim semantics, suggestion
Dim outputLine, cleanLength, dataTypeUpper

' Set output file path — saves to Desktop
filePath = CreateObject("WScript.Shell").SpecialFolders("Desktop") & "\varchar2_semantics_report.csv"

Set fso = CreateObject("Scripting.FileSystemObject")
Set file = fso.CreateTextFile(filePath, True)

' Write CSV header
file.WriteLine "Table Name,Column Name,Data Type,Length,Semantics,Suggestion"

' Loop through all tables
For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        For Each col In tbl.Columns
            dataTypeUpper = UCase(col.DataType)
            If InStr(dataTypeUpper, "VARCHAR2") > 0 Then

                On Error Resume Next
                semantics = col.GetExtendedAttribute("LengthSemantics")
                If Err.Number <> 0 Then
                    semantics = ""
                    Err.Clear
                End If
                On Error GoTo 0

                ' Skip columns already declared as VARCHAR2(n CHAR)
                If InStr(dataTypeUpper, "CHAR") = 0 And Trim(UCase(semantics)) <> "CHAR" Then
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

MsgBox "✅ Report saved to Desktop as 'varchar2_semantics_report.csv' (only discrepancies shown)", vbInformation, "Final CHAR Semantics Report"
